from selenium import webdriver

# configure webdriver
options = webdriver.ChromeOptions()
options.headless = True
options.add_argument("--window-size:1920,1080")
options.add_argument("--start-maximized")

# configure chrome browser to not load images and javascript
chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option(
    "prefs", {"profile.managed_default_content_settings.images": 2}
)

capabilities = options.to_capabilities()
capabilities.update(
    {
    "browser": "chrome",
    "browser_version": "latest",
    "os": "Windows",
    "os_version": "10",
    "build": "Python Sample Build",
    "name": "Pop-ups testing",
    "chromeOptions": {"excludeSwitches": ["disable-popup-blocking"]},
}
)