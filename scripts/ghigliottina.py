import array
from scipy.sparse import coo_array, csc_array, csr_array
from glob import glob

def load_dictionary(whitelist_path):
    f = open(whitelist_path, 'r')
    whitelist = []
    for line in f:
        whitelist.append(line[:-1])
    f.close()

    # create dictionary and inverted dictionary from whitelist
    dictionary = {key: (value + 1) for value, key in enumerate(whitelist)}
    inverted_dict = {value: key for key, value in dictionary.items()}
    
    return dictionary, inverted_dict


def load_model(pmi_path):
    row = array.array('i',[])
    col = array.array('i',[])
    data = array.array('f',[])

    for path in glob(pmi_path + "/part-00***"):
        f = open(path, 'r')
        for line in f:
            values = line.split()
            row.append(int(values[0]))
            col.append(int(values[1]))
            data.append(float(values[2]))
        f.close()    

    model = coo_array((data, (row, col)))
    csc = csc_array(model)
    csr = csr_array(model)

    return csc, csr


def ghigliottina(dictionary, inverted_dict, csc, csr, clues):
    # dict that will hold (solution, value)
    solutions={}
    # dict that will hold (solution, number of times that solution was related to a clue)
    found_solutions={}
    # minimum value, will be added when (clue, solution) are not related to adjust the mean
    min_val=0
    
    for clue in clues:
        clue.lower()
        clue_token = dictionary.get(clue)
        
        if clue_token != None:
            # getting both rows and cols because the matrix is triangular
            csc_sol = csc.getcol(clue_token).tocoo()
            sol1 = [(k, v) for k,v in zip(csc_sol.row,csc_sol.data)]
        
            csr_sol = csr.getrow(clue_token).tocoo()
            sol2 = [(k, v) for k,v in zip(csr_sol.col,csr_sol.data)]
            
            for s, v in sol1+sol2:
                if v < min_val:
                    min_val = v
        
                if solutions.get(s) != None:
                    solutions[s] += v
                    found_solutions[s] += 1
                else:
                    solutions[s] = v
                    found_solutions[s] = 1
    
    # calculating mean
    for sol, val in solutions.items():
        solutions[sol] += min_val * (5 - found_solutions[sol])
        solutions[sol] /= 5
    
    # extracting solutions
    sorted_solutions = sorted(list(solutions.items()), key=lambda x: x[1], reverse=True)
    sorted_solutions = [(inverted_dict[token], value) for token, value in sorted_solutions[:5]]

    return sorted_solutions
    
