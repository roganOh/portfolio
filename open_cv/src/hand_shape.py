import cv2
import numpy as np

import src.preprocessing as preprocessing
from src.color import Color
from src.rock_scissor_paper import Hand_shape as hs


def draw_contours(img, contours):
    hull = cv2.convexHull(contours)  # 몇개의 점이 주어졌을 때 몇개의 점을 골라 나머지 모든 점을 내부에 포함하는 다각형 제작
    cv2.drawContours(img, [hull], -1, Color.magenta.value, 2)  # 등고선 다시 만드는 함수(손의 윤곽선)


def get_defects(contours):
    hull = cv2.convexHull(contours, returnPoints=False)  # 다시 hull 받아서 노란선 그리고
    defects = cv2.convexityDefects(contours, hull)  # defect 3차원 배열로 받아서 점을 찍었다.
    # 각 변 시작점,끝점,가장 먼 지점, 가장 가까운 지점까지의 대략적 거리를 통해 꼭짓점 찾는 과정(return 4개 준다)
    # 육안으로 세었을 때와 컴퓨터는 조금 다르게 각자의 방식으로 숫자 셈.

    return defects


def check_hand_shape(defects, contours, cnt, img, i):
    p1, p2, p3, d = defects[i][0]  # 피타고라스 이용(좌표를 받아온다)
    point1 = tuple(contours[p1][0])
    point2 = tuple(contours[p2][0])
    point3 = tuple(contours[p3][0])
    a = np.sqrt((point2[0] - point1[0]) ** 2 + (point2[1] - point1[1]) ** 2)
    b = np.sqrt((point3[0] - point1[0]) ** 2 + (point3[1] - point1[1]) ** 2)
    c = np.sqrt((point2[0] - point3[0]) ** 2 + (point2[1] - point3[1]) ** 2)  # (삼각형 그린다)
    if b * b + c * c >= a * a:  # 추가설명 입력
        cnt += 1
        cv2.circle(img, point3, 4, [0, 0, 255], -1)
    return cnt


def print_hand_shape(cnt):
    print('', cnt)
    if cnt == hs.rock.value:
        print('바위')
    if cnt == hs.scissor.value:
        print('가위')
    if cnt == hs.paper.value:
        print('보')


def check_hand_shape_and_get_cnt(img):
    contours = preprocessing.get_contours(img)
    draw_contours(img, contours)
    defects = get_defects(contours)

    try:
        _ = defects.shape[0]
    except:
        print("unreadable photo. please check photo")
        exit()
    finally:
        cnt = 0

    for i in range(defects.shape[0]):  # tuple: 자료형 함수(리스트랑 유사)
        cnt = check_hand_shape(defects, contours, cnt, img, i)

    if cnt > 0:
        cnt = cnt + 1

    print_hand_shape(cnt)

    return cnt
