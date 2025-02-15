# Write a program which tell you:
# How many hours are in a year?
# How many minutes are in a decade?
# How many seconds old are you?


def calculation (age):
    total_hr = 24 * 365 
    total_min = 60 * 24 * 365 * 10
    second_old = age* 365 * 24 * 60 * 60

    print("total_hr is " , total_hr)
    print("total_min is " , total_min)
    print("second_old is " , second_old)

calculation (20)
 
