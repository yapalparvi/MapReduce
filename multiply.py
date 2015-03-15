import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
a_row_max=5
b_col_max=5
def mapper(record):
    # key: document identifier
    # value: document contents
    matrix=record[0]
    row=record[1]
    column=record[2]
    value=record[3]
    if matrix =='a':
        for k in range(0,b_col_max):
            mr.emit_intermediate((row,k),[matrix,column,value])
    else:
        for i in range(0,a_row_max):
            mr.emit_intermediate((i,column),[matrix,row,value])


def reducer(key, list_of_values):
    a_values=filter(lambda cell:cell[0]=='a',list_of_values)
    b_values=filter(lambda cell:cell[0]=='b',list_of_values)

    a_set=set(map(lambda s:s[1],a_values))
    b_set=set(map(lambda s:s[1],b_values))
    a_b_set=a_set & b_set

    b_rows= filter(lambda row: row[1] in a_b_set, b_values)
    a_cols = filter(lambda row: row[1] in a_b_set, a_values)

#Multiplying the matching pairs

    a_b_mult = map(lambda x: x[0][2] * x[1][2], zip(b_rows, a_cols))
    mr.emit((key[0],key[1],sum(a_b_mult)))
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
