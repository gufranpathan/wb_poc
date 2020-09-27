from PIL import Image

top_margin = 139
left_margin = 67
width = 503
height = 194
row_margin = 1

def crop_200(image,row,col,page,num_pages):
    top_margin = 116
    top_margin_first_page = 139
    top_margin_secondlast_page = 182
    left_margin = 67
    width = 503
    height = 194
    height_last_two = 207.5
    row_margin = 1

    if page==3:
        top_margin = top_margin_first_page
    elif page==num_pages-2:
        top_margin = top_margin_secondlast_page

    if page>=num_pages-2:
        height = height_last_two


    x = (left_margin + (col - 1) * width, top_margin + (row - 1) * height + (row - 1) * row_margin)
    y = (left_margin + col * width, top_margin + row * height + row * row_margin)

    return image.crop(x + y)
#
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
