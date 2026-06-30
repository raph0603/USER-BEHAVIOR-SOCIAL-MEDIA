"""
Récupère le titre + le corps (selftext) des posts Reddit originaux.

Les requêtes HTTP classiques (requests) sont bloquées par l'anti-bot de Reddit
(403 Cloudflare), et même un Chromium Playwright lancé "à neuf" est bloqué par
le mur anti-bot Akamai (page "You've been blocked by network security").

Comme pour x_scraper.py / x_url_finder.py, on se connecte donc à un Chrome
DEJA OUVERT (idéalement déjà utilisé pour naviguer normalement, et si possible
connecté à un compte Reddit) via le protocole CDP, plutôt que de lancer un
Chromium automatisé tout neuf.

Avant de lancer ce script :
1) Fermer tout Chrome déjà ouvert.
2) Lancer Chrome avec le débogage distant activé, par exemple :
   "C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\chrome-debug-profile"
3) Dans cette fenêtre, aller sur reddit.com (se connecter à un compte si possible).
4) Lancer ce script : il va se connecter à CETTE fenêtre déjà ouverte.

Gestion du rate-limit (HTTP 429) :
- Délai entre requêtes augmenté (3 à 6s).
- En cas de 429 : on NE marque PAS l'URL comme échec définitif, on la remet en
  fin de file et on attend une pause longue (60s, doublée à chaque 429
  consécutif, plafonnée à 5 min) avant de réessayer.
- Reprise automatique : si le fichier de sortie existe déjà, les URLs déjà
  récupérées sont relues et ignorées (on écrit en mode "append"), donc on peut
  relancer le script après une interruption sans tout refaire.
"""
import json
import time
import random
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright

CDP_URL = "http://127.0.0.1:9222"
BASE_DELAY = (3.0, 6.0)
MAX_429_PAUSE = 300  # 5 min


def make_json_url(post_url: str) -> str:
    return post_url.rstrip("/") + ".json?limit=1&raw_json=1"


def parse_listing(result):
    """Retourne (post_dict, None) ou (None, error_str)."""
    if isinstance(result, dict) and "__error" in result:
        return None, f"http_{result['__error']}"
    if not isinstance(result, list) or len(result) < 1:
        return None, "unexpected_format"
    children = result[0].get("data", {}).get("children", [])
    if not children:
        return None, "no_post_data"
    data = children[0].get("data", {})
    title = data.get("title", "") or ""
    selftext = data.get("selftext", "") or ""
    if not title and not selftext:
        return None, "empty_post"
    return {
        "post_url": data.get("permalink", ""),
        "subreddit": data.get("subreddit", ""),
        "title": title,
        "selftext": selftext,
        "score": data.get("score"),
        "created_utc": data.get("created_utc"),
        "is_self": data.get("is_self"),
        "removed_by_category": data.get("removed_by_category"),
        "external_url": data.get("url") if not data.get("is_self") else None,
    }, None


def load_done_urls(output_file: str):
    done = set()
    p = Path(output_file)
    if not p.exists():
        return done
    with open(p, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            u = d.get("post_url")
            if u:
                done.add(u)
    return done


def run(input_file: str, output_file: str):
    all_urls = [l.strip() for l in Path(input_file).read_text(encoding="utf-8").splitlines() if l.strip()]
    done_urls = load_done_urls(output_file)
    urls = [u for u in all_urls if u not in done_urls]
    print(f"[{input_file}] {len(all_urls)} URLs au total, {len(done_urls)} déjà récupérées, {len(urls)} restantes")

    if not urls:
        print("Rien à faire, tout est déjà récupéré.")
        return len(done_urls), 0

    ok, failed = 0, 0
    pause_429 = 60  # pause courante en cas de 429, augmente en cas de répétition

    with sync_playwright() as p:
        print(f"Connexion au Chrome déjà ouvert sur {CDP_URL} ...")
        try:
            browser = p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            print(f"ERREUR : impossible de se connecter à {CDP_URL} ({type(e).__name__}: {e})")
            print("-> Vérifie que Chrome est bien lancé avec --remote-debugging-port=9222")
            sys.exit(1)

        if browser.contexts:
            context = browser.contexts[0]
        else:
            context = browser.new_context()

        page = context.new_page()
        print("Navigation vers reddit.com ...")
        page.goto("https://www.reddit.com", timeout=60000)
        page.wait_for_timeout(3000)

        with open(output_file, "a", encoding="utf-8") as out:
            i = 0
            total = len(urls)
            queue = list(urls)
            while queue:
                url = queue.pop(0)
                i += 1
                json_url = make_json_url(url)
                try:
                    result = page.evaluate(
                        """async (u) => {
                            try {
                                const res = await fetch(u, {headers: {'Accept': 'application/json'}});
                                if (!res.ok) return {__error: res.status};
                                return await res.json();
                            } catch (e) {
                                return {__error: 'fetch_exception'};
                            }
                        }""",
                        json_url,
                    )
                except Exception as e:
                    result = {"__error": f"js_error:{type(e).__name__}"}

                post, err = parse_listing(result)

                if err == "http_429":
                    # rate-limit : on remet l'URL en fin de file, pas un échec définitif
                    queue.append(url)
                    print(f"  [{i}] 429 rate-limit -> pause {pause_429}s, '{url}' remise en file ({len(queue)} restantes)")
                    time.sleep(pause_429)
                    pause_429 = min(pause_429 * 2, MAX_429_PAUSE)
                    continue
                else:
                    pause_429 = 60  # reset après un succès ou une autre erreur

                if post:
                    post["post_url"] = url  # on garde l'URL d'origine, pas le permalink relatif
                    out.write(json.dumps(post, ensure_ascii=False) + "\n")
                    out.flush()
                    ok += 1
                else:
                    failed += 1
                    print(f"  [{i}] ECHEC ({err}) : {url}")

                if (ok + failed) % 25 == 0:
                    print(f"  ... {ok + failed}/{total} traités ({ok} ok, {failed} échecs, {len(queue)} en attente)")

                time.sleep(random.uniform(*BASE_DELAY))

        page.close()
        # on ne ferme PAS browser.close() : c'est le Chrome de l'utilisateur, pas le nôtre.

    print(f"[{input_file}] terminé : {ok} posts récupérés, {failed} échecs -> {output_file}")
    return ok, failed


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python reddit_post_scraper.py <input_urls.txt> <output.jsonl>")
        sys.exit(1)
    run(sys.argv[1], sys.argv[2])
