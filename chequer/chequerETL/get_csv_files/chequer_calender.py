"""
https://asciiart.website/index.php?art=events/birthday
"""
from datetime import datetime
import base64
import requests
from icalendar import Calendar
from notion.client import *
import pymsteams
from PIL import Image
from io import BytesIO

def happy_birthday():
    admin = ''
    pwd = ''
    url = ''
    notion_token = ''
    notion_url = ''
    client = NotionClient(token_v2=notion_token)
    page = client.get_collection_view(notion_url)

    aggregations = [{
        "id": "Name"
    }]
    collections = page.build_query(aggregate=aggregations).execute()
    ids = collections.collection.get_rows()
    chequer_crew=[]
    for row in collections.collection.get_rows():
        chequer_crew.append(format(str(row.title).split(' ')[0]))
    print(chequer_crew)


    response = requests.get(url, auth=(admin, pwd))
    response = response.text
    gcal = Calendar.from_ical(response)
    today = datetime.now()
    this_year = today.year
    birthday_person =""
    for component in gcal.walk():
        if component.name == "VEVENT":
            getting = component.get('summary')
            people = getting[:-3]
            if "생일" in getting:
                if people in chequer_crew:
                    birthday = datetime.strptime(str(this_year) + "-" + str(component.get('dtstart').dt.month) + "-" + str(
                        component.get('dtstart').dt.day), '%Y-%m-%d')
                    if today.date() == birthday.date():
                        print("축하합니다 " + people + " 생일")
                        birthday_person = people

    buf= BytesIO()
    fig=Image.open("/home/ec2-user/airflow/dags/get_csv_files/cake.jpeg")
    fig.save(buf, fig.format, quality=25)
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    encoded_fig = f"data:image/jpeg;base64,{data}"
    if birthday_person:
        year = str(datetime.now().year)
        month = str(datetime.now().month)
        day = str(datetime.now().day)
        myTeamsMessage = pymsteams.connectorcard(
            "")
        myTeamsMessage.text(year + " 년  " + month + " 월  " + day + " 일")
        teamsSection = pymsteams.cardsection()
        teamsSection.addImage(encoded_fig)
        myTeamsMessage.addSection(teamsSection)
        myTeamsMessage.title("오늘은 " + birthday_person + " 의 생일입니다!! ")
        myTeamsMessage.send()