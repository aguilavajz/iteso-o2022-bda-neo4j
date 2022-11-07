#!/usr/bin/env python3
import csv
import os

from neo4j import GraphDatabase
from neo4j.exceptions import ConstraintError

class TwitterApp(object):

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._create_constraints()

    def close(self):
        self.driver.close()

    def _create_constraints(self):
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT unique_user IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE")
            session.run("CREATE CONSTRAINT unique_subreddit IF NOT EXISTS FOR (s:Subreddit) REQUIRE s.name IS UNIQUE")

    def _create_user_node(self, username, followers, karma):
        with self.driver.session() as session:
            try:
                session.run("CREATE (u:User {username: $username, num_of_followers: $followers, karma: $karma})", username=username, followers=followers, karma=karma)
            except ConstraintError:
                print("User already exists: ", username)

    def _create_subreddit_node(self, name, description,subscribers):
        with self.driver.session() as session:
            try:
                session.run("CREATE (s:Subreddit {name: $name, description: $description, total_subscribers: $subscribers})", name=name, description=description,subscribers=subscribers)
            except ConstraintError:
                print("Subreddit already exists: ", name)

    def _create_post_node(self, title, text, karma):
        with self.driver.session() as session:
            try:
                session.run("CREATE (p:Post {title: $title, text: $text, karma: $karma})", title=title,text=text,karma=karma)
            except ConstraintError:
                pass

    def _create_comment_node(self, text,karma):
        with self.driver.session() as session:
            try:
                session.run("CREATE (c:Comment {text: $text, karma: $karma})", text=text,karma=karma)
            except ConstraintError:
                pass

    def _create_subscribes_relationship(self, username, subreddit):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (s:Subreddit)
                WHERE u.username=$username AND s.name=$subreddit
                CREATE (u)-[r:SUBSCRIBES]->(s)
                RETURN type(r)""", username=username, subreddit=subreddit)


    def _create_moderates_relationship(self, username, subreddit):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (s:Subreddit)
                WHERE u.username=$username AND s.name=$subreddit
                CREATE (u)-[r:MODERATES]->(s)
                RETURN type(r)""", username=username, subreddit=subreddit)

    def _create_published_relationship(self, username, post):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (p:Post)
                WHERE u.username=$username AND p.title=$post
                CREATE (u)-[r:FROM]->(p)
                RETURN type(r)""", username=username, post=post)

    def _create_downvotes_relationship(self, username, post):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (p:Post)
                WHERE u.username=$username AND p.title=$post
                CREATE (u)-[r:DOWNVOTES]->(p)
                RETURN type(r)""", username=username, post=post)

    def _create_upvotes_relationship(self, username, post):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (p:Post)
                WHERE u.username=$username AND p.title=$post
                CREATE (u)-[r:UPVOTES]->(p)
                RETURN type(r)""", username=username, post=post)

    def _create_commented_relationship(self, username, comment):
        with self.driver.session() as session:
            session.run("""
                MATCH (u:User), (c:Comment)
                WHERE u.username=$username AND c.text=$comment
                CREATE (u)-[r:COMMENTED]->(c)
                RETURN type(r)""", username=username, comment=comment)

    def init(self, source):
        with open(source, newline='') as csv_file:
            reader = csv.DictReader(csv_file,  delimiter=',')
            for r in reader:
                self._create_user_node(r["username"],r["followers"],r["user_karma"])
                if r["description"] != '':
                    self._create_subreddit_node(r["subreddit_name"], r["description"],r["subscribers"])
                if r["type"] == 'Subscribes':
                    self._create_subscribes_relationship(r["username"], r["subreddit_name"])
                if r["type"] == 'Moderates':
                    self._create_moderates_relationship(r["username"], r["subreddit_name"])
                if r["type"] == 'Post':
                    self._create_post_node(r["title"],r["post_text"],r["post_karma"])
                    self._create_published_relationship(r["username"], r["title"])
                if r["type"] == 'Comment':
                    self._create_comment_node(r["comment_text"],r["comment_karma"])
                    self._create_commented_relationship(r["username"], r["comment_text"])
                if r["type"] == 'Upvote':
                    self._create_downvotes_relationship(r["username"], r["title"])
                if r["type"] == 'Downvote':
                    self._create_upvotes_relationship(r["username"], r["title"])

if __name__ == "__main__":
    # Read connection env variables
    neo4j_uri = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
    neo4j_user = os.getenv('NEO4J_USER', 'neo4j')
    neo4j_password = os.getenv('NEO4J_PASSWORD', 'T4t13584#')

    twitter = TwitterApp(neo4j_uri, neo4j_user, neo4j_password)
    twitter.init("data/reddit.csv")

    twitter.close()

