Users =[	
  "userid",
  "age",
  "gender",
  "occupation",
  "zipcode",
]


Zipcodes = [
  "zipcode",
  "zipcodetype",
  "city",
  "state",
]
def userszipcode():
    mr=[]
    mr.append('Mapper Input')
    mr.append('Mapper 1 Input: '+','.join(Users))
    mr.append('Mapper 1 Output:')
    mr.append('Key: '+Users[4])
    mr.append('Value: '+','.join(Users))
    
    mr.append('Mapper 2 Input: '+','.join(Zipcodes))
    mr.append('Mapper 2 Output:')
    mr.append('Key:'+Zipcodes[0])
    mr.append('Value:'+','.join(Zipcodes))
    
    mr.append('Sorting and Shuffling')

    mr.append('Reducer Input: Mapper 1 and Mapper 2 Output belonging to same Key '+Users[4])
    mr.append('Reducer Output:')
    str2 = ""
    str2 = str2 + Users[0]
    #mr.append(Users[0])
    for x in Users[1:]:
        str2 = str2 + ","+x
    for y in Zipcodes[1:]:
        str2 = str2 +"," + y
    mr.append(str2)
    dict_key = {"Map_Reduce_Operations_Chain":mr}
    return dict_key