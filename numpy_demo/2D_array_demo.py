import numpy as np;

arr = np.arange(0, 25);
print("Original Array:");
print(arr);

print("\nReshaped Array (5x5):");   
arr_2d = arr.reshape(5, 5);
print(arr_2d);

print("Array Shape:");
print(arr_2d.shape);


print("sum of all elements:");
print(np.sum(arr_2d));

print("sum of each column:");
print(np.sum(arr_2d, axis=0));  

print("sum of each row:");
print(np.sum(arr_2d, axis=1));