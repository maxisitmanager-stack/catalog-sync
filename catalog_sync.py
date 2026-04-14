"""
catalog_sync.py
===============
Notion DB に添付されたカタログPDFを検知し、
SSH(SFTP)経由でレンタルサーバーに自動転送して公開URLをNotionに書き戻す。

■ Render の Environment Variables に以下を設定してください:
  NOTION_TOKEN      : Notion Integration のシークレットトークン
  NOTION_DB_ID      : カタログDBのID（URLの末尾32文字）
  SSH_HOST          : サーバーのホスト名またはIPアドレス
  SSH_USER          : SSHログインユーザー名
  SSH_PASSWORD      : SSHパスワード
  SSH_REMOTE_PATH   : アップロード先パス (例: /var/www/html/catalog)
  PUBLIC_URL_BASE   : 公開URL のベース (例: https://example.com/catalog)

■ 任意の環境変数（デフォルト値あり）:
  SSH_PORT          : SSHポート番号（デフォルト: 22）
  POLL_INTERVAL     : チェック間隔（秒）（デフォルト: 300 = 5分）
  PROP_PDF_FILE     : PDF添付プロパティ名（デフォルト: カタログPDF）
  PROP_PUBLIC_URL   : URL書き戻しプロパティ名（デフォルト: 公開URL）
  PROP_SYNC_STATUS  : 同期ステータスプロパティ名（デフォルト: 同期ステータス）
  PROP_TITLE        : タイトルプロパティ名（デフォルト: 名称）
"""

import io
import logging
import os
import time

import paramiko
import requests

# ---------------------------------------------------------------------------
# 設定（環境変数から読み込む）
# ---------------------------------------------------------------------------
NOTION_TOKEN    = os.environ["NOTION_TOKEN"]
NOTION_DB_ID    = os.environ["NOTION_DB_ID"]
SSH_HOST        = os.environ["SSH_HOST"]
SSH_PORT        = int(os.environ.get("SSH_PORT", "22"))
SSH_USER        = os.environ["SSH_USER"]
SSH_PASSWORD    = os.environ["SSH_PASSWORD"]
SSH_REMOTE_PATH = os.environ["SSH_REMOTE_PATH"]   # 末尾スラッシュなし
PUBLIC_URL_BASE = os.environ["PUBLIC_URL_BASE"]   # 末尾スラッシュなし

POLL_INTERVAL   = int(os.environ.get("POLL_INTERVAL", "300"))

# Notionプロパティ名（NotionDBの列名と合わせてください）
PROP_PDF_FILE    = os.environ.get("PROP_PDF_FILE",    "カタログPDF")
PROP_PUBLIC_URL  = os.environ.get("PROP_PUBLIC_URL",  "公開URL")
PROP_SYNC_STATUS = os.environ.get("PROP_SYNC_STATUS", "同期ステータス")
PROP_TITLE       = os.environ.get("PROP_TITLE",       "名称")

# ---------------------------------------------------------------------------
# ログ設定
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ---------------------------------------------------------------------------
# Notion 操作
# ---------------------------------------------------------------------------

def get_pending_pages() -> list[dict]:
    """「同期ステータス」が空 or「未同期」のページを取得する"""
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    body = {
        "filter": {
            "or": [
                {
                    "property": PROP_SYNC_STATUS,
                    "select": {"is_empty": True},
                },
                {
                    "property": PROP_SYNC_STATUS,
                    "select": {"equals": "未同期"},
                },
            ]
        }
    }
    resp = requests.post(url, headers=NOTION_HEADERS, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json().get("results", [])


def extract_pdf_files(page: dict) -> list[dict]:
    """ページのファイルプロパティからPDF情報（name, url）を抽出する"""
    props = page.get("properties", {})
    file_prop = props.get(PROP_PDF_FILE, {})
    raw_files = file_prop.get("files", [])

    results = []
    for f in raw_files:
        name = f.get("name", "catalog.pdf")
        if f.get("type") == "file":
            results.append({"name": name, "url": f["file"]["url"]})
        elif f.get("type") == "external":
            results.append({"name": name, "url": f["external"]["url"]})
    return results


def get_page_title(page: dict) -> str:
    """ページのタイトルを取得する"""
    try:
        title_list = page["properties"][PROP_TITLE]["title"]
        return title_list[0]["plain_text"] if title_list else "（タイトルなし）"
    except (KeyError, IndexError):
        return "（タイトルなし）"


def update_notion_page(page_id: str, public_url: str, status: str) -> None:
    """NotionページのURLとステータスを更新する"""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    props: dict = {
        PROP_SYNC_STATUS: {"select": {"name": status}},
    }
    if public_url:
        props[PROP_PUBLIC_URL] = {"url": public_url}

    resp = requests.patch(url, headers=NOTION_HEADERS, json={"properties": props}, timeout=30)
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# ファイル操作
# ---------------------------------------------------------------------------

def download_file(url: str) -> bytes:
    """指定URLからファイルをダウンロードする（Notionの署名付きURLに対応）"""
    headers = NOTION_HEADERS if "secure.notion-static.com" in url or "prod-files-secure" in url else {}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.content


def upload_via_sftp(data: bytes, filename: str) -> str:
    """SFTPでサーバーにアップロードし、公開URLを返す"""
    transport = paramiko.Transport((SSH_HOST, SSH_PORT))
    try:
        transport.connect(username=SSH_USER, password=SSH_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)
        remote_path = f"{SSH_REMOTE_PATH.rstrip('/')}/{filename}"
        sftp.putfo(io.BytesIO(data), remote_path)
        sftp.close()
    finally:
        transport.close()

    public_url = f"{PUBLIC_URL_BASE.rstrip('/')}/{filename}"
    return public_url


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def run_once() -> None:
    """1サイクルの同期処理"""
    log.info("同期チェックを開始します...")
    pages = get_pending_pages()
    log.info(f"{len(pages)} 件の未同期ページを検出しました")

    for page in pages:
        page_id    = page["id"]
        page_title = get_page_title(page)
        log.info(f"▶ 処理開始: {page_title}")

        pdf_files = extract_pdf_files(page)
        if not pdf_files:
            log.warning(f"  PDFファイルが見つかりません（スキップ）: {page_title}")
            continue

        last_url = ""
        all_ok = True
        for pdf_info in pdf_files:
            filename = pdf_info["name"]
            try:
                log.info(f"  ダウンロード中: {filename}")
                pdf_data = download_file(pdf_info["url"])

                log.info(f"  サーバーへアップロード中: {SSH_HOST} → {SSH_REMOTE_PATH}/{filename}")
                last_url = upload_via_sftp(pdf_data, filename)
                log.info(f"  アップロード完了: {last_url}")

            except Exception as exc:
                log.error(f"  エラー（{filename}）: {exc}")
                all_ok = False

        status = "同期済み" if all_ok else "エラー"
        update_notion_page(page_id, last_url, status)
        log.info(f"  Notion 更新完了 → ステータス: {status}")

    log.info("同期チェック完了")


def main() -> None:
    log.info("=" * 60)
    log.info("カタログ自動同期サービスを開始しました")
    log.info(f"  Notion DB : {NOTION_DB_ID}")
    log.info(f"  SSH HOST  : {SSH_HOST}:{SSH_PORT}")
    log.info(f"  リモートパス: {SSH_REMOTE_PATH}")
    log.info(f"  公開URLベース: {PUBLIC_URL_BASE}")
    log.info(f"  ポーリング間隔: {POLL_INTERVAL} 秒")
    log.info("=" * 60)

    while True:
        try:
            run_once()
        except Exception as exc:
            log.error(f"予期しないエラーが発生しました: {exc}", exc_info=True)

        log.info(f"{POLL_INTERVAL} 秒後に再チェックします...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
