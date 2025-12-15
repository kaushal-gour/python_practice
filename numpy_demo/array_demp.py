import numpy as np;
arr = np.arange(0,10);
print(arr);

arr = arr +5;
print(arr);


arr = arr -2;
print(arr);

print
log_arr = np.log(arr);
print(log_arr);

print("Maximum Value:");
max = arr.max();
print(max);

print("Minimum Value:");
min = arr.min();
print(min);

#standard deviation
print("Standard Deviation:");
std = arr.std();
print(std);

#variance
print("Variance:");
var = arr.var();
print(var);