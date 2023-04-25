import os
import glob

"""
This python script is to clear all the nodes at the time of debugging 
"""

def main():
    all_nodes = glob.glob('/home/dscd/map-reduce/nodes/node*')
    for node in all_nodes:
        os.system(f'rm -rf {node}/mapper/input/*.txt')
        os.system(f'rm -rf {node}/mapper/output/*.txt')
        os.system(f'rm -rf {node}/mapper/*.py')
        os.system(f'rm -rf {node}/reducer/input/*.txt')
        os.system(f'rm -rf {node}/reducer/output/*.txt')
        os.system(f'rm -rf {node}/reducer/*.py')

if __name__=='__main__':
    main()  