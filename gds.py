#!/usr/bin/env python3
import csv
import os

from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError
from graphdatascience import GraphDataScience

neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
neo4j_password = os.getenv('NEO4J_PASSWORD', 'T4t13584#')

gds = GraphDataScience(neo4j_uri, auth=(neo4j_user, neo4j_password))

print(gds.version())
assert gds.version()

name1 = input("Enter name 1: ")
name2 = input("Enter name 2: ")

node1 = gds.find_node_id(["Person"], {"name": name1})
node2 = gds.find_node_id(["Person"], {"name": name2})

score = gds.alpha.linkprediction.totalNeighbors(node1, node2)
print(round(score, 2))
#assert round(score, 2) == 0.62

gds.close()