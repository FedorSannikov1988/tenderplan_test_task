import time
import requests
import xmltodict
from celery import Celery
from bs4 import BeautifulSoup


URL_FOR_REQUESTS: str = "https://zakupki.gov.ru/epz/order/extendedsearch/results.html?fz44=on&pageNumber="
URL_FOR_PRINT_XML: str = "https://zakupki.gov.ru/epz/order/notice/printForm/viewXml.html?regNumber="
URL_FOR_SEARCH_ON_PAGE: str = "/epz/order/notice/printForm/view.html?regNumber="
NUMBER_OF_ATTEMPTS_CONNECT: int = 5
PAUSE_SEK: int = 2


app = Celery('site_parsing')


@app.task(bind=True, max_retries=NUMBER_OF_ATTEMPTS_CONNECT)
def requesting_data_from_print_form(self, reg_number: str) -> None:

    link_print_xml: str = URL_FOR_PRINT_XML + reg_number

    repetition_counter: int = 1

    while repetition_counter != NUMBER_OF_ATTEMPTS_CONNECT + 1:

        try:
            response = requests.get(link_print_xml)

            if response.ok:

                xml_doc = xmltodict.parse(response.text)

                start_key: str = list(xml_doc.keys())[0]

                all_doc: dict = xml_doc.get(start_key)

                if all_doc:

                    common_info: dict = all_doc.get('commonInfo')

                    if common_info:

                        publish_dt_in_eis: dict = common_info.get('publishDTInEIS')
                        print(f'link: {link_print_xml} , publishDTInEIS: {publish_dt_in_eis} ;')

                    else:

                        print(f'link: {link_print_xml} , commonInfo: None ;')

                return None

            else:
                # вместо логера:
                print(f"link: {link_print_xml} , "
                      f"Статус запроса: {response.status_code} , "
                      f"Попытка № {repetition_counter} ;")

        except ConnectionError as error:
            # вместо логера:
            print(f"link: {link_print_xml} , Ошибка: {error}")
            self.retry(exc=error, countdown=PAUSE_SEK)

        repetition_counter += 1

        # что бы не получить 429:
        time.sleep(PAUSE_SEK)


@app.task(bind=True, max_retries=NUMBER_OF_ATTEMPTS_CONNECT)
def requesting_data_from_page(self, page_namber: str) -> None:

    repetition_counter: int = 1

    link_page: str = URL_FOR_REQUESTS + page_namber

    while repetition_counter != NUMBER_OF_ATTEMPTS_CONNECT + 1:

        try:
            response = requests.get(link_page)

            if response.ok:

                soup = BeautifulSoup(response.text, 'html.parser')

                for all_tag_a in soup.find_all('a', href=True):

                    if URL_FOR_SEARCH_ON_PAGE in all_tag_a['href']:

                        reg_number: str = \
                            str(all_tag_a['href']).split(URL_FOR_SEARCH_ON_PAGE)[1]

                        print(reg_number)

                        requesting_data_from_print_form.delay(reg_number)

                return None

            else:
                # вместо логера:
                print(f"link: {link_page} , "
                      f"Статус запроса: {response.status_code} , "
                      f"Попытка № {repetition_counter} ;")

        except ConnectionError as error:
            # вместо логера:
            print(f"link: {link_page} , Ошибка: {error}")
            self.retry(exc=error, countdown=PAUSE_SEK)

        repetition_counter += 1

        # что бы не получить 429:
        time.sleep(PAUSE_SEK)


def start_parsing(start: int, stop: int) -> None:

    for num_page in range(start, stop+1):
        requesting_data_from_page.delay(str(num_page))


if __name__ == "__main__":
    start_parsing(start=1, stop=2)

