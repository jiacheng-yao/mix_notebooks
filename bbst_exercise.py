import numpy as np

class BinaryTree():
    def __init__(self, nodevalue):
      self.left = None
      self.right = None
      self.nodevalue = nodevalue

    def getLeftChild(self):
        return self.left
    def getRightChild(self):
        return self.right
    def setNodeValue(self,value):
        self.nodevalue = value
    def getNodeValue(self):
        return self.nodevalue

    def insertRight(self,newNode):
        self.right = newNode

    def insertLeft(self,newNode):
        self.left = newNode

def printTree(tree):
        if tree != None:
            printTree(tree.getLeftChild())
            print(tree.getNodeValue())
            printTree(tree.getRightChild())

def build_tree(l, start, end):
    if start > end:
        return None

    mid = (start+end)/2
    root = BinaryTree(l[mid])

    root.insertLeft(build_tree(l, start, mid-1))
    root.insertRight(build_tree(l, mid+1, end))

    return root

l_int = range(50)

result_tree = build_tree(l_int, 0, len(l_int)-1)