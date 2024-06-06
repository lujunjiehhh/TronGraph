import time

from lxml import html
from playwright.sync_api import sync_playwright
import pymongo


class TagCreeper:
    def __init__(self):

        self.db = pymongo.MongoClient("mongodb://127.0.0.1:27017/")["address_tag"]
        self.playwright = sync_playwright().start()
        # 设置代理
        launch_options = {"proxy": {"server": "http://127.0.0.1:7890"}}
        self.browser = self.playwright.chromium.launch(**launch_options)  # type: ignore
        self.page = self.browser.new_page()
        times = 0
        while times < 10:
            try:
                self.page.goto(
                    "https://www.oklink.com/cn/trx/address/TM1zzNDZD2DPASbKcgdVoTYhfmYgtfwx9R"
                )
                break
            except:
                times += 1
                time.sleep(0.5)
        self.page.goto(
            "https://www.oklink.com/cn/trx/address/TM1zzNDZD2DPASbKcgdVoTYhfmYgtfwx9R"
        )

    def search(self, query_address):

        collection = self.db["address_tag"]
        if query_address in collection.distinct("address"):
            # 查看是否在db中存在tag
            document = collection.find_one({"address": query_address})
            tag = ""
            try:
                tag = document.get("tag")
            except Exception as ee:
                print(ee)
            return tag
        else:
            self.page.fill(".innerInput_XbdzX", "")
            self.page.fill(".innerInput_XbdzX", query_address)
            time.sleep(0.4)
            _content = self.page.content()
            # 输出到文件中
            tree = html.fromstring(_content)
            selected_elements = tree.xpath(
                '/html/body/header/div/div/div/div[3]/div/div/div/div[1]/div/div[2]/div/div[2]/a/div/div[2]/div/span/span'
            )


            def insert_into_db(address_tag):
                data = {"address": query_address, "tag": address_tag}
                try:
                    collection.insert_one(data)
                except Exception as e:
                    print(e)
            if selected_elements:
                tag: str = selected_elements[0].text
                if tag.startswith('Wallet') or tag.startswith('Bitpie') or tag.startswith('Gambling') or tag.startswith(
                        'imToken'):
                    insert_into_db("无标签")
                    return "无标签"
                else:
                    insert_into_db(tag)
                    return tag
            else:
                insert_into_db("无标签")
                return "无标签"

    def close(self):
        self.browser.close()
        self.playwright.stop()


if __name__ == "__main__":
    browser_handler = TagCreeper()

    while address := input("请输入要查询的地址:"):
        content = browser_handler.search(address)
        print(content)

    browser_handler.close()
