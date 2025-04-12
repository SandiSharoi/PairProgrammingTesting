from class_etl import transform_data, load_data

if __name__ == "__main__":
    df = transform_data()
    load_data(df,"class_demo_table")