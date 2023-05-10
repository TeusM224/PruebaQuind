import pandas as pd
import numpy as np

def filterCategories(df):

    df['category'] = df['category'].apply(lambda s: str(s).lower()) #if isinstance(s, list) else 'NO DATA')
    new_df = pd.DataFrame(columns=df.columns)

    """A continuación, se pasa una lista de palabras relacionadas a restaurantes, hoteles, parques y zonas turisticas o de recreación"""

    keywords = ['hotel', 'motel', 'hostel', 'breakfast', 'b&b', 'inn', 'resort', 'lodging', 'pension',
                'room', 'suite', 'chamber', 'palace', 'cabin', 'mansion', 'apartment', 'mansion',
                'guesthouse', 'boutique hotel', 'spas', 'beach hotel', 'ski hotel', 'casino',
                'pet-friendly hotel', 'luxury hotel', 'historic hotel', 'bed & breakfast',
                'family-friendly hotel', 'airport hotel', 'business hotel', 'cheap hotel',
                'all-inclusive resort', 'design hotel', 'floating hotel', 'health resort', 'hostel', 'boutique resort'

                'restaurant', 'bar', 'diner', 'bbq', 'pizza', 'burger', 'sandwich', 'dining', 'grill', 'dinner',
                'cafeteria', 'barbecue', 'tavern', 'delicatessen', 'food', 'coffee', 'buffet', 'bakery', 'pub',
                'cafe', 'steakhouse', 'bistro', 'gastropub', 'brewery', 'winery', 'tapas', 'sushi', 'seafood',
                'brunch', 'ramen', 'noodle', 'vegetarian', 'vegan', 'fast food', 'ice cream', 'dessert', 'creperie'

                "Nature reserves", "Botanical gardens", "Arboretums", "National parks", 
                "State parks", "Forest preserves", "Greenways", "Land trusts", "Wildlife", 
                "Wetlands", "Beaches", "Picnic areas", "Recreation areas", "Campgrounds", "Amusement parks", "Water parks", 
                "Skate parks", "Dog parks", "Sports fields", "Golf courses", "Tennis courts", "Hiking trails", "Bike paths", 
                "Nature trails", "Scenic overlooks", "Lakeshores", "Riverbanks",

                "Amusement park", "Water park", "Zoo", "Botanical garden", "Nature trail", "Hiking trail",
                "Camping ground", "Picnic area", "Skate park", "Roller skating rink", "Ski resort",
                "Snowboarding park", "Golf course", "Miniature golf", "Tennis", "Basketball", "Baseball",
                "Soccer field", "Football field", "Playground", "Splash pad", "Community center", "Recreation center", "Sports complex",
                "Theme park", "State park", "National park", "Conservation area"]

    for keyword in keywords:
        filter = df[df['category'].str.contains(keyword)]
        new_df = pd.concat([new_df, filter])

    new_df.drop_duplicates(subset='gmap_id', inplace=True)

    new_df['ID_meta'] = np.arange(new_df.shape[0])

    new_df.to_csv('/content/drive/MyDrive/pruebaQuind/data/processed/filtered_meta.csv', sep=';', index = False, mode='a')

    return new_df

#filterCategories(df_meta)