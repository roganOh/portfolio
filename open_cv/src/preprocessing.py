import cv2
import numpy as np

from color import Color

def resize(img):
    img = cv2.resize(img, (512, 512))
    img1 = img[0:512, 0:256]
    img2 = img[0:512, 256:512]

    return img,img1,img2

def get_contours(img):
    hsvim = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)  # 파란색,녹색,빨간색 이미지를 (HSV)색조,채도,값으로 변경
    lower = np.array([0, 48, 80], dtype="uint8")  # HSV에서 피부색의 낮은 범위,2의32승으로 값 제한
    upper = np.array([20, 255, 255], dtype="uint8")  # HSV에서 피부색의 상위 범위
    skinRegionHSV = cv2.inRange(hsvim, lower, upper)  # 피부감지
    blurred = cv2.blur(skinRegionHSV, (2, 2))  # 이미지를 흐리게 만듬(masking 개선)
    ret, thresh = cv2.threshold(blurred, 0, 255, cv2.THRESH_BINARY)

    contours, hierarchy = cv2.findContours(thresh, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    # findContours: 이미지에서 등고선 찾는 함수, hierarchy 추출모드 1개, contour 근사모드 1개- 테두리 추출
    contours = max(contours, key=lambda x: cv2.contourArea(x))
    # lambda: for문역할(x:cv.contourArea(x)를 x 하나하나에 적용, contourArea: 폐곡선 면적 구하는 함수
    cv2.drawContours(img, [contours], -1, Color.green.value, 2)
    # The function draws contour outlines in the image
    # contourIdx:-1, color, thickness

    return contours