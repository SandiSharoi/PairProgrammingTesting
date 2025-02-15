
# Write a program that answer this question:
# If I’m 1406 million seconds old, how old am I?

def toYear(sec):
    age = round(sec/(365 * 24 * 60 * 60))
    print("My age is ", age)

toYear(1406000000)