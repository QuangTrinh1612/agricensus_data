import agricensus_api
import config

if __name__ == '__main__':
    
    # Daily Export Data
    agricensus_api.get_agricensus_data(config.token, feed_type='daily_export', file_format='CSV', file_path='Data')