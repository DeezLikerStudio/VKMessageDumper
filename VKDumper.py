import os
import re
import json
import requests
import vk_api
from vk_api.exceptions import ApiError
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs

MAX_WORKERS = 6
COUNT_PER_REQUEST = 200
TOKEN_FILE = "token.json"
STATE_FILE = "resume_state.json"


def ensure_folder(path):
    os.makedirs(path, exist_ok=True)


def extract_token_from_oauth_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.fragment:
        raise ValueError
    params = parse_qs(parsed.fragment)
    token = params.get("access_token", [None])[0]
    if not token:
        raise ValueError
    return token


def load_token():
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r", encoding="utf-8") as f:
                return json.load(f).get("access_token")
        except Exception:
            return None
    return None


def save_token(token: str):
    try:
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump({"access_token": token}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def is_token_valid(token: str) -> bool:
    try:
        vk = vk_api.VkApi(token=token).get_api()
        vk.users.get()
        return True
    except Exception:
        return False


def get_valid_token():
    token = load_token()
    if token and is_token_valid(token):
        return token

    while True:
        try:
            oauth_url = input("OAuth URL: ").strip()
            token = extract_token_from_oauth_url(oauth_url)
            if is_token_valid(token):
                save_token(token)
                return token
            print("Invalid token")
        except Exception:
            print("Invalid OAuth URL")


def extract_peer_id_from_link(url: str) -> int:
    match = re.search(r"/convo/(-?\d+)", url)
    if not match:
        raise ValueError
    return int(match.group(1))


def validate_peer_id(peer_id: int):
    if peer_id < 0:
        raise ValueError("Dialogs with bots or communities are not supported")


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def download_photo(task):
    url, path = task
    if os.path.exists(path):
        return False
    try:
        r = requests.get(url, stream=True, timeout=20)
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        return True
    except Exception:
        return False


def input_conversation():
    while True:
        try:
            convo_link = input("Conversation link: ").strip()
            folder = input("Output folder: ").strip()

            peer_id = extract_peer_id_from_link(convo_link)
            validate_peer_id(peer_id)

            return peer_id, folder

        except ValueError as e:
            print(f"❌ {e or 'Invalid conversation link'}")


def main():
    try:
        token = get_valid_token()
        peer_id, folder = input_conversation()

        ensure_folder(folder)

        vk = vk_api.VkApi(token=token).get_api()

        state = load_state()
        start_from = state.get(str(peer_id))

        progress = tqdm(desc="Photos", unit="items")
        total = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while True:
                try:
                    params = {
                        "peer_id": peer_id,
                        "media_type": "photo",
                        "count": COUNT_PER_REQUEST
                    }
                    if start_from:
                        params["start_from"] = start_from

                    resp = vk.messages.getHistoryAttachments(**params)

                except ApiError as e:
                    print(f"\nVK API error: {e}")
                    break
                except Exception:
                    print("\nUnexpected error while requesting VK API")
                    break

                items = resp.get("items", [])
                if not items:
                    break

                tasks = []
                for item in items:
                    try:
                        photo = item["attachment"]["photo"]
                        sizes = photo.get("sizes", [])
                        if not sizes:
                            continue
                        best = max(sizes, key=lambda s: s.get("width", 0))
                        url = best["url"]
                        ext = url.split("?")[0].split(".")[-1]
                        filename = f"photo_{photo['id']}.{ext}"
                        path = os.path.join(folder, filename)
                        tasks.append((url, path))
                    except Exception:
                        continue

                futures = [executor.submit(download_photo, t) for t in tasks]
                for f in as_completed(futures):
                    if f.result():
                        total += 1
                        progress.update(1)

                start_from = resp.get("next_from")
                state[str(peer_id)] = start_from
                save_state(state)

                if not start_from:
                    state.pop(str(peer_id), None)
                    save_state(state)
                    break

        progress.close()
        print(f"Done. Downloaded: {total}")

    except KeyboardInterrupt:
        print("\nInterrupted by user")


if __name__ == "__main__":
    main()
