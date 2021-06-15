import cv2

import src.hand_shape
import src.preprocessing
from src.color import Color
from src.rock_scissor_paper import Hand_shape as hs
from src.win import Win

def result(cnt1,cnt2):
    if cnt1 == cnt2:
        print(Win.no_one.value)
    if cnt1 == hs.paper.value:
        if cnt2 == hs.scissor.value:
            print(Win.person2.value)
        if cnt2 == hs.rock.value:
            print(Win.person1.value)
    if cnt1 == hs.scissor.value:
        if cnt2 == hs.paper.value:
            print(Win.person1.value)
        if cnt2 == hs.rock.value:
            print(Win.person2.value)
    if cnt1 == hs.rock.value:
        if cnt2 == hs.scissor.value:
            print(Win.person1.value)
        if cnt2 == hs.paper.value:
            print(Win.person2.value)

img = cv2.imread("./photo/rock2.jpg")
# img = cv2.imread("./{바꾸셈}/rock2.jpg")
img, img1, img2 = preprocessing.resize(img)

cnt1 = hand_shape.check_hand_shape_and_get_cnt(img1)
cnt2 = hand_shape.check_hand_shape_and_get_cnt(img2)

result(cnt1,cnt2)

cv2.putText(img1, str(cnt1), (0, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, Color.red.value, 2, cv2.LINE_AA)
cv2.putText(img2, str(cnt2), (0, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, Color.red.value, 2, cv2.LINE_AA)
cv2.imshow('final_result', img)
