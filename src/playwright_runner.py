import asyncio
from playwright.async_api import async_playwright

async def scrape_with_playwright(branches_input, scrape_func):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        result = await scrape_func(page, branches_input)
        await browser.close()
        return result
