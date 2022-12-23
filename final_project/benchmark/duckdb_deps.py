# created this class to facilitate the benchmarking and loading the data from the feather files

def load_data():
    import pandas as pd
    academic = pd.read_feather('../data/feathers/academic.feather')

    # Tables concerning inproceedings (conference articles)
    improceeding = pd.read_feather(
        '../data/feathers/improceeding_table.feather')
    author_inproceeding = pd.read_feather(
        '../data/feathers/author_inproceedings.feather')
    booktitle_improceeding = pd.read_feather(
        '../data/feathers/improceeding_booktitle.feather')

    # All tables concerning articles(journal articles)
    article = pd.read_feather('../data/feathers/articles_table.feather')
    author_article = pd.read_feather('../data/feathers/author_article.feather')
    journal = pd.read_feather('../data/feathers/journals_table.feather')

    # All tables concerning proceedings (conference proceedings => bundled conference articles)
    editor_proceeding = pd.read_feather(
        '../data/feathers/editor_proceedings.feather')
    publisher_proceeding = pd.read_feather(
        '../data/feathers/publisher_proceeding.feather')
    publisher = pd.read_feather('../data/feathers/publisher_table.feather')
    proceeding = pd.read_feather('../data/feathers/proceeding_table.feather')
    booktitle_proceeding = pd.read_feather(
        '../data/feathers/proceeding_booktitle.feather')

    # All tables concerning booktiles (conferences)
    booktitle = pd.read_feather('../data/feathers/booktitles_table.feather')

    return verify_data(academic, improceeding, author_inproceeding, booktitle_improceeding, article, author_article, journal, editor_proceeding, publisher_proceeding, publisher, proceeding, booktitle_proceeding, booktitle)


def verify_data(academic, improceeding, author_inproceeding, booktitle_improceeding, article, author_article, journal, editor_proceeding, publisher_proceeding, publisher, proceeding, booktitle_proceeding, booktitle):
    # remove duplicated rows from all tables if they exist
    improceeding = improceeding.drop_duplicates(keep='first')
    author_inproceeding = author_inproceeding.drop_duplicates(keep='first')
    booktitle_improceeding = booktitle_improceeding.drop_duplicates(
        keep='first')
    article = article.drop_duplicates(keep='first')
    author_article = author_article.drop_duplicates(keep='first')
    journal = journal.drop_duplicates(keep='first')
    editor_proceeding = editor_proceeding.drop_duplicates(keep='first')
    publisher_proceeding = publisher_proceeding.drop_duplicates(keep='first')
    publisher = publisher.drop_duplicates(keep='first')
    proceeding = proceeding.drop_duplicates(keep='first')
    booktitle_proceeding = booktitle_proceeding.drop_duplicates(keep='first')
    booktitle = booktitle.drop_duplicates(keep='first')

    # rename columns of tables to be more readable for the querries
    academic.columns = ['academic_id', 'academic_name']
    article.columns = ['article_id', 'article_title', 'journal_id']
    author_article.columns = ['academic_id', 'article_id']
    journal.columns = ['journal_id', 'journal_name']
    proceeding.columns = ['proceeding_id',
                          'proceeding_title', 'year', 'volume']
    publisher.columns = ['publisher_id', 'publisher_name']
    booktitle.columns = ['booktitle_id', 'booktitle_name']
    booktitle_proceeding.columns = ['proceeding_id', 'booktitle_id']
    improceeding.columns = ['improceeding_id',
                            'improceeding_title', 'pages', 'year']
    booktitle_improceeding.columns = ['improceeding_id', 'booktitle_id']
    publisher_proceeding.columns = ['publisher_id', 'proceeding_id']
    editor_proceeding.columns = ['academic_id', 'proceeding_id']
    author_inproceeding.columns = ['academic_id', 'improceeding_id']

    return academic, improceeding, author_inproceeding, booktitle_improceeding, article, author_article, journal, editor_proceeding, publisher_proceeding, publisher, proceeding, booktitle_proceeding, booktitle


def init_duckdb():
    # create the duckdb database in memory

    import duckdb
    import pandas as pd
    conn = duckdb.connect()
    cursor = conn.cursor()

    # create tables

    # academic table
    cursor.execute('CREATE TABLE academic AS SELECT * FROM academic')

    # Create the 'improceeding' table
    cursor.execute('CREATE TABLE improceeding AS SELECT * FROM improceeding')

    # Create the 'author_inproceeding' table
    cursor.execute(
        'CREATE TABLE author_inproceeding AS SELECT * FROM author_inproceeding')

    # Create the 'booktitle_improceeding' table
    cursor.execute(
        'CREATE TABLE booktitle_improceeding AS SELECT *  FROM booktitle_improceeding')

    # Create the 'article' table
    cursor.execute('CREATE TABLE article AS SELECT * FROM article')

    # Create the 'author_article' table
    cursor.execute(
        'CREATE TABLE author_article AS SELECT * FROM author_article')

    # Create the 'journal' table
    cursor.execute('CREATE TABLE journal AS SELECT * FROM journal')

    # Create the 'editor_proceeding' table
    cursor.execute(
        'CREATE TABLE editor_proceeding AS SELECT * FROM editor_proceeding')

    # Create the 'publisher_proceeding' table
    cursor.execute(
        'CREATE TABLE publisher_proceeding AS SELECT * FROM publisher_proceeding')

    # Create the 'publisher' table
    cursor.execute('CREATE TABLE publisher AS SELECT * FROM publisher')

    # Create the 'proceeding' table
    cursor.execute('CREATE TABLE proceeding AS SELECT * FROM proceeding')

    # Create the 'booktitle_proceeding' table
    cursor.execute(
        'CREATE TABLE booktitle_proceeding AS SELECT * FROM booktitle_proceeding')

    # Create the 'booktitle' table
    cursor.execute('CREATE TABLE booktitle AS SELECT * FROM booktitle')

    return conn, cursor


def close_duckdb(conn, cursor):
    # Close the cursor and connection
    cursor.close()
    conn.close()


# duckdb final querries :
E1 = """
SELECT publisher.publisher_name 
FROM publisher 
JOIN publisher_proceeding ON publisher.publisher_id=publisher_proceeding.publisher_id 
JOIN proceeding ON proceeding.proceeding_id=publisher_proceeding.proceeding_id 
WHERE proceeding.proceeding_title LIKE '%PODS%' AND publisher.publisher_name IS NOT NULL
GROUP BY publisher_name
"""

E2 = """
SELECT article.article_title 
FROM article JOIN author_article ON author_article.article_id=article.article_id 
JOIN academic ON academic.academic_id=author_article.academic_id 
JOIN journal ON article.journal_id=journal.journal_id 
WHERE journal.journal_name='Theory Comput. Syst.' 
AND academic.academic_name='Martin Grohe' 
ORDER BY article.article_title
"""


M1 = """
            SELECT COUNT(year) as number_of_articles_published, year 
            FROM booktitle 
            JOIN booktitle_improceeding ON booktitle_improceeding.booktitle_id=booktitle.booktitle_id 
            JOIN improceeding ON improceeding.improceeding_id=booktitle_improceeding.improceeding_id 
            WHERE lower(booktitle.booktitle_name) LIKE '%sigmod%' 
            AND year='2022' 
            GROUP BY year
            """

M2 = """
SELECT journal_name,
            COUNT(journal_name) as num_articles
            FROM article
            JOIN journal ON article.journal_id = journal.journal_id
            WHERE journal.year = (SELECT MIN(journal.year)
                                    FROM journal)
            GROUP BY journal_name
            ORDER BY num_articles DESC
            LIMIT 1
"""


M3 = """
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY frequency) as median_frequency
FROM (
SELECT count(improceeding.improceeding_id) as frequency, improceeding.year AS improceedings_per_year
FROM booktitle
JOIN booktitle_improceeding ON booktitle_improceeding.booktitle_id = booktitle.booktitle_id
JOIN improceeding ON improceeding.improceeding_id = booktitle_improceeding.improceeding_id
WHERE booktitle.booktitle_name = 'CIDR'
GROUP BY improceeding.year
ORDER BY improceeding.year ASC
)
"""


M4 = """
SELECT MAX(improceeding_frequency_10) as number_of_SIGMOD_conference_higher_than_10_authors, ANY_VALUE(year) as year
FROM (
SELECT COUNT(year) as improceeding_frequency_10, year
FROM improceeding
JOIN booktitle_improceeding ON booktitle_improceeding.improceeding_id = improceeding.improceeding_id
JOIN booktitle ON booktitle.booktitle_id = booktitle_improceeding.booktitle_id
WHERE booktitle.booktitle_name LIKE '%SIGMOD%'
GROUP BY year
HAVING COUNT(*) > 10
ORDER BY improceeding_frequency_10 DESC
)
"""


M5 = """
            SELECT academic_name, author_frequency FROM
            (SELECT COUNT(academic.academic_name) AS author_frequency, academic.academic_name 
            FROM academic 
            JOIN editor_proceeding ON academic.academic_id=editor_proceeding.academic_id 
            JOIN proceeding ON proceeding.proceeding_id=editor_proceeding.proceeding_id 
            JOIN booktitle_proceeding ON proceeding.proceeding_id=booktitle_proceeding.proceeding_id
            JOIN booktitle ON booktitle.booktitle_id=booktitle_proceeding.booktitle_id
            WHERE booktitle.booktitle_name LIKE '%PODS%' 
            GROUP BY academic.academic_name)
            WHERE author_frequency = (SELECT MAX(author_frequency) FROM     (SELECT COUNT(academic.academic_name) AS author_frequency, academic.academic_name 
                                                                            FROM academic 
                                                                            JOIN editor_proceeding ON academic.academic_id=editor_proceeding.academic_id 
                                                                            JOIN proceeding ON proceeding.proceeding_id=editor_proceeding.proceeding_id 
                                                                            JOIN booktitle_proceeding ON proceeding.proceeding_id=booktitle_proceeding.proceeding_id
                                                                            JOIN booktitle ON booktitle.booktitle_id=booktitle_proceeding.booktitle_id
                                                                            WHERE booktitle.booktitle_name LIKE '%PODS%' 
                                                                            GROUP BY academic.academic_name))"""

M6 = """
            SELECT COUNT(booktitle_name) AS number_of_different_conferences_published FROM
            (SELECT COUNT(booktitle.booktitle_name), booktitle.booktitle_name FROM booktitle
            JOIN booktitle_improceeding ON booktitle_improceeding.booktitle_id=booktitle.booktitle_id
            JOIN improceeding ON improceeding.improceeding_id=booktitle_improceeding.improceeding_id
            JOIN author_inproceeding ON author_inproceeding.improceeding_id=improceeding.improceeding_id
            JOIN academic ON academic.academic_id=author_inproceeding.academic_id
            WHERE academic.academic_id=(SELECT A.academic_id
                                        FROM
                                        (SELECT MAX(Z.total_publishings), ANY_VALUE(Z.academic_id) AS academic_id
                                        FROM
                                        (SELECT X.academic_id, number_of_improceedings, number_of_journal_articles, number_of_improceedings+number_of_journal_articles as total_publishings
                                        FROM 
                                            (SELECT COUNT(author_inproceeding.academic_id) as number_of_improceedings, author_inproceeding.academic_id 
                                            FROM academic 
                                            JOIN author_inproceeding ON author_inproceeding.academic_id=academic.academic_id 
                                            GROUP BY author_inproceeding.academic_id) X 
                                            JOIN 
                                                (SELECT COUNT(author_article.academic_id) as number_of_journal_articles, author_article.academic_id 
                                                FROM academic 
                                                JOIN author_article ON author_article.academic_id=academic.academic_id 
                                                GROUP BY author_article.academic_id) Y 
                                            ON X.academic_id=Y.academic_id ORDER BY total_publishings DESC) Z
                                        WHERE NOT(Z.academic_id=-1)) A)
            GROUP BY booktitle.booktitle_name)
            """


H1 = """
                SELECT ANY_VALUE(a1.academic_id), ANY_VALUE(a1.academic_name), ANY_VALUE(a2.academic_name) as coauthor, COUNT(*) as coauthor_count
                FROM author_article aa1
                JOIN author_article aa2 ON aa1.article_id = aa2.article_id
                JOIN academic a1 ON aa1.academic_id = a1.academic_id
                JOIN academic a2 ON aa2.academic_id = a2.academic_id
                WHERE a2.academic_id != a1.academic_id
                GROUP BY a1.academic_id, a2.academic_name
                ORDER BY a1.academic_id, coauthor_count DESC
                LIMIT 5
            """

H2 = """
WITH RECURSIVE co_publishers AS (
  SELECT a1.academic_id, a2.academic_id AS co_publisher_id, 0 AS distance
  FROM academic a1
  JOIN author_inproceeding ai1 ON a1.academic_id = ai1.academic_id
  JOIN author_inproceeding ai2 ON ai1.improceeding_id = ai2.improceeding_id AND ai2.academic_id <> a1.academic_id
  JOIN academic a2 ON ai2.academic_id = a2.academic_id
  WHERE a1.academic_name = 'Maurizio Lenzerini'
  UNION ALL
  SELECT cp.academic_id, a.academic_id AS co_publisher_id, cp.distance + 1 AS distance
  FROM co_publishers cp
  JOIN author_inproceeding ai1 ON cp.co_publisher_id = ai1.academic_id
  JOIN author_inproceeding ai2 ON ai1.improceeding_id = ai2.improceeding_id AND ai2.academic_id <> cp.co_publisher_id
  JOIN academic a ON ai2.academic_id = a.academic_id
)
SELECT MIN(distance) AS distance
FROM co_publishers
WHERE co_publisher_id IN (SELECT academic_id FROM academic WHERE academic_name = 'Martin Grohe')
            """

B1 = """
        SELECT a1.academic_name, a2.academic_name
        FROM academic a1
        JOIN editor_proceeding ep1 ON a1.academic_id = ep1.academic_id
        JOIN proceeding p ON ep1.proceeding_id = p.proceeding_id
        JOIN editor_proceeding ep2 ON p.proceeding_id = ep2.proceeding_id
        JOIN academic a2 ON ep2.academic_id = a2.academic_id
        WHERE a1.academic_id <> a2.academic_id
"""


def get_duckdb_queries():
    return [E1, E2, M1, M2, M3, M4, M5, M6]


def get_duckdb_queries_names():
    return ["E1", "E2", "M1", "M2", "M3", "M4", "M5", "M6", ]
