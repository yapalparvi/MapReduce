import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    seq= record[0]
    nucleotides= record[1]
    trimmed=nucleotides[:len(nucleotides)-10]
    mr.emit_intermediate(trimmed,seq)

def reducer(key, list_of_values):
    mr.emit(key)
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
