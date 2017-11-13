from collections import deque

def search(lines, pattern,history = 5):
    previous_lines = deque(maxlen = history)
    for l1 in lines:
        if pattern in l1:
            yield l1, previous_lines
        previous_lines.append(l1)

if __name__ == '__main__':
    with open(r'somefile.txt') as f:
        for line,prevlines in search(f,'python',1):
            for pline in prevlines:
                print(pline, end = ' ') 
            print(line, end = ' ')
            print('-'*20)
