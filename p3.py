def compareTriplets (x, y):

    a = 0
    b = 0
    i = 0

    while( i < len(x) ):
        
        if x[i] > y[i]:
            a = a+1
        
        elif y[i] > x[i]:
            b = b + 1

        i = i+1

    return[a,b]

answer = compareTriplets([1,2,3],[3,2,1])
print(answer)