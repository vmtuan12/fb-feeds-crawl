from consumer_worker.consumer_worker import ConsumerWorker

if __name__ == "__main__":
    try:
        c = ConsumerWorker()
        c.start()
    except Exception as e:
        print(e)
    finally:
        c.clean_up()