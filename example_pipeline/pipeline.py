from pipelines import tasks, Pipeline


NAME = 'test_project'
VERSION = '2023'


TASKS = [
    tasks.LoadFile(input_file='o1.csv', table='original2'),
    
    # tasks.LoadFile(input_file='original/original.csv', table='original'),
    # tasks.CTAS(
    #     table='norm',
    #     sql_query='''
    #         select *, domain_of_url(url)
    #         from original;
    #     '''
    # ),
    tasks.CopyToFile(
        table='original2',
        output_file='norm1',
    ),

    # # clean up:
    # tasks.RunSQL('drop table original'),
    # tasks.RunSQL('drop table norm'),
    # tasks.RunSQL('CREATE TABLE original (id INTEGER PRIMARY KEY, name TEXT NOT NULL, url text NOT NULL)'),
    # tasks.RunSQL('INSERT INTO sqlitedb_developers3 (id, name, url, domain_of_url) VALUES(1, "teg", "hello", "hi")'),
]


pipeline = Pipeline(
    name=NAME,
    version=VERSION,
    tasks=TASKS
)


if __name__ == "__main__":
    # 1: Run as script
    pipeline.run()

    # 2: Run as CLI
    # > pipelines run
