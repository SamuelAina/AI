assignments = []

def assign_value(values, box, value):
    """
    Please use this function to update your values dictionary!
    Assigns a value to a given box. If it updates the board record it.
    """

    # Don't waste memory appending actions that don't actually change any values
    if values[box] == value:
        return values

    values[box] = value
    if len(value) == 1:
        assignments.append(values.copy())
    return values

def value_occurs_twice(v, vl):
    "routine for checking the an element value v occurs exactly 2 times in the list vl"    

    c =0
    for vi in vl:
        if vi==v:
            c=c+1
    if c==2:             
        return True 
    else:
        return False

"TESTING MY ROUTINE value_occurs_twice"
assert value_occurs_twice(2, [1,3,4,5,5,2,2]) == True 
assert value_occurs_twice(2, [2,1,3,4,5,5,2,2]) == False
assert value_occurs_twice(5, [1,3,4,5,5,2,2,4,4,4,6,8]) == True     


def naked_twins(values):
    """Eliminate values using the naked twins strategy.
    Args:
        values(dict): a dictionary of the form {'box_name': '123456789', ...}

    Returns:
        the values dictionary with the naked twins eliminated from peers.
    """
    
    """
    Loop thru all boxes for those that contain only 2 values. 
    if within any unit, there are such 2-valued boxes having the
    exact values then eliminate those values from the rest of the
    boxes in that unit.
    """   
    #loop tru all boxes
    for unit in unitlist:
        # Find all instances of naked twins
        boxes_with_2_items= [box for box in unit if len(values[box]) == 2] 
        
        #proceed if there are more than one boxes with 2 items
        if len(boxes_with_2_items)>= 2:
            
            #put the values contaned in the 2-value boxes in a list 
            valuelist = []

            for b in boxes_with_2_items:            
                valuelist.append(values[b])
           
            #get a set of boxes that contain exactly 2 items 
            #and where there are two such boxes in the current unit
            twins = [b2 for b2 in boxes_with_2_items if value_occurs_twice(values[b2],valuelist)]
            if len(twins)>0:
                #go through other boxes that are not naked twins and remove twin numbers
                non_twins = [box for box in unit if box not in twins]
                
                #loop through all the sets of twins
                for tw in twins:
                    for nt_b in (non_twins):
                        values[nt_b]=values[nt_b].replace(values[tw][0], "")
                        values[nt_b]=values[nt_b].replace(values[tw][1], "")   
    return values    
    
    
rows = 'ABCDEFGHI'
cols = '123456789'
    
def cross(A, B):
    "Cross product of elements in A and elements in B."
    return [s+t for s in A for t in B]

boxes = cross(rows, cols)
row_units = [cross(r, cols) for r in rows]
column_units = [cross(rows, c) for c in cols]
square_units = [cross(rs, cs) for rs in ('ABC','DEF','GHI') for cs in ('123','456','789')]


#Add diagonal units 
diagonal_units = [['A1','B2','C3','D4','E5','F6','G7','H8','I9'],['I1','H2','G3','F4','E5','D6','C7','B8','A9'] ]


unitlist = row_units + column_units + square_units + diagonal_units 

units = dict((s, [u for u in unitlist if s in u]) for s in boxes)
peers = dict((s, set(sum(units[s],[]))-set([s])) for s in boxes)

def display(values):
    """
    Display the values as a 2-D grid.
    Input: The sudoku in dictionary form
    Output: None
    """
    width = 1+max(len(values[s]) for s in boxes)
    line = '+'.join(['-'*(width*3)]*3)
    for r in rows:
        print(''.join(values[r+c].center(width)+('|' if c in '36' else '')
                      for c in cols))
        if r in 'CF': print(line)
    return

def grid_values(grid):
    """
    Convert grid into a dict of {square: char} with '123456789' for empties.
    Args:
        grid(string) - A grid in string form.
    Returns:
        A grid in dictionary form
            Keys: The boxes, e.g., 'A1'
            Values: The value in each box, e.g., '8'. If the box has no value, then the value will be '123456789'.
    """
    values = []
    all_digits = '123456789'
    for c in grid:
        if c == '.':
            values.append(all_digits)
        elif c in all_digits:
            values.append(c)
    assert len(values) == 81
    return dict(zip(boxes, values))



def eliminate(values):
    """Eliminate values from peers of each box with a single value.

    Go through all the boxes, and whenever there is a box with a single value,
    eliminate this value from the set of values of all its peers.

    Args:
        values: Sudoku in dictionary form.
    Returns:
        Resulting Sudoku in dictionary form after eliminating values.
    """
    solved_values = [box for box in values.keys() if len(values[box]) == 1]
    for box in solved_values:
        digit = values[box]
        for peer in peers[box]:
            values[peer] = values[peer].replace(digit,'')
    return values

def only_choice(values):
    for unit in unitlist:
        for digit in '123456789':
            dplaces = [box for box in unit if digit in values[box]]
            if len(dplaces) == 1:
                values[dplaces[0]] = digit
    return values

def reduce_puzzle(values):
    stalled = False
    while not stalled:
        # Check how many boxes have a determined value
        solved_values_before = len([box for box in values.keys() if len(values[box]) == 1])
        # Use the Eliminate Strategy
        values = eliminate(values)
        # Use the Only Choice Strategy
        values = only_choice(values)
        
        # Use the naked twin strtegy
        values = naked_twins(values)
        
        # Check how many boxes have a determined value, to compare
        solved_values_after = len([box for box in values.keys() if len(values[box]) == 1])
        # If no new values were added, stop the loop.
        stalled = solved_values_before == solved_values_after
        # Sanity check, return False if there is a box with zero available values:
        if len([box for box in values.keys() if len(values[box]) == 0]):
            return False
    return values

def search(values):
    "Using depth-first search and propagation, try all possible values."
    # First, reduce the puzzle using the previous function
    values = reduce_puzzle(values)
    if values is False:
        return False ## Failed earlier
    if all(len(values[s]) == 1 for s in boxes): 
        return values ## Solved!
    # Choose one of the unfilled squares with the fewest possibilities
    n,s = min((len(values[s]), s) for s in boxes if len(values[s]) > 1)
    # Now use recurrence to solve each one of the resulting sudokus, and 
    for value in values[s]:
        new_sudoku = values.copy()
        new_sudoku[s] = value
        attempt = search(new_sudoku)
        if attempt:
            return attempt

def solve(grid):
    """
    Find the solution to a Sudoku grid.
    Args:
        grid(string): a string representing a sudoku grid.
            Example: '2.............62....1....7...6..8...3...9...7...6..4...4....8....52.............3'
    Returns:
        The dictionary representation of the final sudoku grid. False if no solution exists.
    """
    return search(grid_values(grid))
    
    
if __name__ == '__main__':
    diag_sudoku_grid = '2.............62....1....7...6..8...3...9...7...6..4...4....8....52.............3'
    display(solve(diag_sudoku_grid))

    """    
    try:
        from visualize import visualize_assignments
        visualize_assignments(assignments)

    except SystemExit:
        pass
    except:
        print('We could not visualize your board due to a pygame issue. Not a problem! It is not a requirement.')
    """

