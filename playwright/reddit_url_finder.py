from playwright.sync_api import sync_playwright

SUBREDDIT_URLS = [
    "https://www.reddit.com/r/electricvehicles/top/?t=year",
    "https://www.reddit.com/r/electricvehicles/top/?t=month",
    "https://www.reddit.com/r/teslamotors/top/?t=year",
    "https://www.reddit.com/r/electricvehicles/search/?q=road+trip&type=link",
    "https://www.reddit.com/r/electricvehicles/search/?q=charging+network&type=link",
    "https://www.reddit.com/r/electricvehicles/search/?q=price&type=link",
    "https://www.reddit.com/r/electricvehicles/search/?q=battery+degradation&type=link",
    "https://www.reddit.com/r/electricvehicles/search/?q=hyundai+ioniq+5&type=link"
]

OUTPUT_FILE = "reddit_urls.txt"
SCROLL_ATTEMPTS = 15

def normalize_url(url):
    if not url:
        return None
    if url.startswith("/r/"):
        url = "https://www.reddit.com" + url
    return url.split("?")[0].rstrip("/")

def find_reddit_urls():
    all_post_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        for subreddit in SUBREDDIT_URLS:
            print(f"\nExploration de : {subreddit}")
            try:
                page.goto(subreddit, timeout=60000)
                page.wait_for_timeout(3000)
            except Exception as e:
                print(f"Erreur chargement : {e}")
                continue

            for _ in range(SCROLL_ATTEMPTS):
                page.mouse.wheel(0, 4000)
                page.wait_for_timeout(2000)

            links_loc = page.locator("a[slot='full-post-link']")
            count = links_loc.count()
            print(f"Trouvé {count} liens sur cette page")

            for i in range(count):
                try:
                    link = links_loc.nth(i).get_attribute("href")
                    clean_link = normalize_url(link)
                    if clean_link:
                        all_post_urls.add(clean_link)
                except:
                    pass

        browser.close()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for url in sorted(all_post_urls):
            f.write(url + "\n")

    print(f"\n[SUCCÈS] {len(all_post_urls)} URLs uniques enregistrées dans {OUTPUT_FILE}")

if __name__ == "__main__":
    find_reddit_urls()