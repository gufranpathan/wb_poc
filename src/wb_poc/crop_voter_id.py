import cv2
import numpy as np
from PIL import Image

# top_margin = 139
# left_margin = 67
# width = 503
# height = 194
# row_margin = 1

def crop_200(image,row,col,page,num_pages,consolidated_page_started,consolidated_page):
    top_margin = 116
    top_margin_first_page = 139
    #top_margin_secondlast_page = 182 # Tested for Bihar and WB
    top_margin_secondlast_page = 210 # For Bangalore (English)
    left_margin = 67
    width = 503
    height = 194
    height_last_two = 207.5
    row_margin = 1

    if page==3:
        top_margin = top_margin_first_page
    if consolidated_page_started:
        height = height_last_two
    if consolidated_page:
        top_margin = top_margin_secondlast_page

    # if page>=num_pages-2:
    #     height = height_last_two


    x = (left_margin + (col - 1) * width, top_margin + (row - 1) * height + (row - 1) * row_margin)
    y = (left_margin + col * width, top_margin + row * height + row * row_margin)

    return image.crop(x + y)


def get_rectanges(image_file):
    #image = cv2.imread(image_file)
    image = np.array(image_file)

    result = image.copy()
    gray = cv2.cvtColor(image,cv2.COLOR_BGR2GRAY)
    thresh = cv2.adaptiveThreshold(gray,255,cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV,51,9)
    cnts = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if len(cnts) == 2 else cnts[1]

    for c in cnts:
        cv2.drawContours(thresh, [c], -1, (255,255,255), -1)
    # Morph open
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (9,9))
    opening = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel, iterations=4)
    # Draw rectangles
    cnts = cv2.findContours(opening, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cnts = cnts[0] if len(cnts) == 2 else cnts[1]
    x_list = []
    y_list = []
    w_list = []
    h_list = []
    for c in cnts:
        x,y,w,h = cv2.boundingRect(c)
        x_list.append(x)
        y_list.append(y)
        w_list.append(w)
        h_list.append(h)
        cv2.rectangle(image, (x, y), (x + w, y + h), (36,255,12), 3)
    #print(y_list)
    return min(y_list) if y_list else 0



# from PIL import Image
# # Page 2 onwards
# top_margin = 116
# left_margin = 67
# width = 503
# height = 194
# row_margin = 1
#
# image = 'data/kaliaganj/images/assembly=34/part=1/dpi=200/page_image/page=4of29.jpg'
# crop_200(Image.open(image),row=10,col=3).save('data/test.jpg')
#
# #second last page
# top_margin = 182
# left_margin = 67
# width = 503
# height = 207.5
# row_margin = 1
#
# image = 'data/kaliaganj/images/assembly=34/part=1/dpi=200/page_image/page=27of29.jpg'
# crop_200(Image.open(image),row=10,col=2).save('data/test.jpg')
#
# #last page
# top_margin = 116
# left_margin = 67
# width = 503
# height = 207.5
# row_margin = 1
#
# image = 'data/kaliaganj/images/assembly=34/part=1/dpi=200/page_image/page=28of29.jpg'
# crop_200(Image.open(image),row=4,col=2).save('data/test.jpg')
#


