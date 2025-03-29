import pandas as pd
import numpy as np
import plugins.utils as utils
from plugins.config import aws_creds, snow_creds
from typing import Optional
import ast

class MovieDatasetEtl:
    def __init__(self, snow_db, snow_schema, kaggle_path = "rounakbanik/the-movies-dataset"):
        """Save path of the data and initialize S3ParquetHandler"""
        self.kaggle_path = kaggle_path
        self.s3_handler = utils.S3ParquetHandler(aws_creds)
        self.snow_handler = utils.SnowflakeHandler(snow_creds, snow_db, snow_schema)

    def extract(self, file_dictionary: str):
        """
        Extract data from CSV files in the specified directory.
        
        Args:
            file_directory (str): Path to the directory containing CSV files
        
        Returns:
            tuple: DataFrames for credits, keywords, links, movies_metadata, and ratings
        """
        
        # Download data from Kaggle or read from a local CSV file
        utils.download_kaggle_dataset(self.kaggle_path, file_dictionary)
        
        # Extract data from the CSV file
        self.credits = pd.read_csv(f"{file_dictionary}/credits.csv")
        self.keywords = pd.read_csv(f"{file_dictionary}/keywords.csv")
        self.links = pd.read_csv(f"{file_dictionary}/links.csv")
        self.movies_metadata = pd.read_csv(f"{file_dictionary}/movies_metadata.csv")
        self.ratings = pd.read_csv(f"{file_dictionary}/ratings.csv")
        
        return (
            self.credits, 
            self.keywords, 
            self.links, 
            self.movies_metadata, 
            self.ratings
        )

    def transform(self):
        """
        Transform and clean the extracted data. Got additional DataFrame from the as described:
        credits 
            - New: Cast and crew data
            - Removed: credits
        movies_metadata 
            - New: belongs_to_collection, genres, production_companies, production_countries, spoken_languages
        
        Returns:
            tuple: DataFrames for 
                cast, crew, keywords, links, movies_metadata, 
                belongs_to_collection, genres, production_companies, 
                production_countries, spoken_languages, and ratings
        """
        # Transform the data as needed
        cast_cleaned, crew_cleaned = self._transform_credits()
        keywords_cleaned = self._transform_keywords()
        links_cleaned = self._transform_links()
        metadata_transform_result = self._transform_movies_metadata()
        movies_metadata_cleaned = metadata_transform_result['movies_metadata']
        belongs_to_collection_cleaned = metadata_transform_result['belongs_to_collection']
        genres_cleaned = metadata_transform_result['genres'] 
        production_companies_cleaned = metadata_transform_result['production_companies'] 
        production_countries_cleaned = metadata_transform_result['production_countries'] 
        spoken_languages_cleaned = metadata_transform_result['spoken_languages']
        # Ratings are good to go as is
        ratings_cleaned = self._transform_ratings()
        
        return {
            'cast': cast_cleaned,
            'crew': crew_cleaned,
            'keywords': keywords_cleaned,
            'links': links_cleaned,
            'movies_metadata': movies_metadata_cleaned,
            'belongs_to_collection': belongs_to_collection_cleaned,
            'genres': genres_cleaned,
            'production_companies': production_companies_cleaned,
            'production_countries': production_countries_cleaned,
            'spoken_languages': spoken_languages_cleaned,
            'ratings': ratings_cleaned
        }

    def load(self, df_lists: dict, snow_stage: str):
        """
        Load all of the dictionary of the DataFrame into amazon S3.
        
        Returns:
            bool: status of the loading proccess
        """
        try:
            # Load the data into aws s3
            for key, df in df_lists.items():
                # Define your S3 bucket and file keys
                destination_bucket = "project-etl-iqbal"
                destination_key = f"etl/{key}.parquet"
                # Write the DataFrame back to S3 as a Parquet file
                self.s3_handler.write_parquet_to_s3(df, destination_bucket, destination_key)
                print(f"Data successfully written to s3://{destination_bucket}/{destination_key}")
            # Create table that needed
            for name, df in df_lists.items():
                self.snow_handler.create_table(name, df)
                self.snow_handler.load(name, snow_stage, f"{name}.parquet")
            print("All of the data loaded successfully")
            self.snow_handler.close()
        except Exception as e:
            print(f"Loading failed reason: {e}")
            return False
        
        return True
    
    def _transform_credits(self):
        """
        Transform and clean the credits data. 
        
        Returns:
            tuple: DataFrames for cast and crew
        """
        df = self.credits.copy()
        
        # Rename column using the same convention
        df.rename(columns={"id": "tmdb_id"}, inplace=True)
        
        # Normalize the cast and crew data
        cast = self._normalize_data(df, "cast", "tmdb_id")
        crew = self._normalize_data(df, "crew", "tmdb_id")
        
        # Data cleaning process for cast and crew
        cast.drop_duplicates(subset=["cast_id", "credit_id", "tmdb_id", "name"], keep="first", inplace=True)
        crew.drop_duplicates(subset=["id", "credit_id", "tmdb_id", "name"], keep="first", inplace=True)
        
        return (cast, crew)
    
    def _transform_keywords(self):
        """
        Transform and clean the keywords data. 
        
        Returns:
            tuple: DataFrames
        """
        df = self.keywords.copy()
        
        # Rename column using the same convention
        df.rename(columns={"id": "tmdb_id"}, inplace=True)
        
        # Normalize the keywords data
        df = self._normalize_data(df, "keywords", "tmdb_id")
        
        # Data cleaning process for keywords
        df.drop_duplicates(keep="first", inplace=True)
        
        return df

    def _transform_links(self):
        """
        Transform and clean the links data. 
        
        Returns:
            tuple: DataFrames
        """
        df = self.links.copy()
        
        # Rename column using the same convention
        new_name = {
            "movieId": "movie_id",
            "imdbId": "imdb_id",
            "tmdbId": "tmdb_id"
            }
        df.rename(columns=new_name, inplace=True)
        
        # Normalize the keywords data
        df["tmdb_id"] = df.tmdb_id.fillna(0).astype(int)
        
        return df


    def _transform_movies_metadata(self):
        """
        Transform and clean the movies_metadata. 
        
        Returns:
            tuple: DataFrames of movies_metadata, 
                belongs_to_collection, genres, production_companies, 
                production_countries, spoken_languages
        """
        df = self.movies_metadata.copy()
        
        # Rename column using the same convention
        df.rename(columns={"id": "tmdb_id"}, inplace=True)
        
        # Convert columns to appropriate data types
        # Remove rows with missing values
        mask = df[["revenue", "runtime", "video", "title", "vote_count"]].isna().all(axis=1)
        df = df[~mask]
        # Convert adult category into boolean type
        df["adult"] = df.adult.astype(bool)
        # Convert budget category into integer type
        df["budget"] = df.budget.astype(int)
        # Convert ID into integer type
        df["tmdb_id"] = df.tmdb_id.astype(int)
        # Convert popularity category into float type
        df["popularity"] = df.popularity.astype(float)
        # Convert release_date into datetime type
        df["release_date"] = pd.to_datetime(df.release_date)
        # Convert revenue category into integer type
        df["revenue"] = df.revenue.astype(int)
        # Convert video category into boolean type
        df["video"] = df.video.astype(bool)
        # Convert vote_count category into integer type
        df["vote_count"] = df.vote_count.astype(int)
        
        # Drop duplicates
        df.drop_duplicates(subset=["tmdb_id", "title", "imdb_id"], keep="first", inplace=True)
        
        # Drop unnecessary columns & extract belongs_to_collection data
        belongs_to_collection = df[["belongs_to_collection", "tmdb_id"]].copy()
        belongs_to_collection = self._extract_dict_values(belongs_to_collection, "belongs_to_collection")
        df.drop("belongs_to_collection", axis=1, inplace=True)
        
        # Remove rows with missing values
        mask = belongs_to_collection[["id", "name"]].isna().all(axis=1)
        belongs_to_collection = belongs_to_collection[~mask]
        
        # Extract genres data
        genres = self._normalize_data(df, "genres", "tmdb_id")
        df.drop(columns=["genres"], inplace=True)
        
        # Extract production_companies data
        production_companies = self._normalize_data(df, "production_companies", "tmdb_id")
        df.drop(columns=["production_companies"], inplace=True)
        
        # Extract production_countries data
        production_countries = self._normalize_data(df, "production_countries", "tmdb_id")
        df.drop(columns=["production_countries"], inplace=True)
        
        # Extract spoken_languages data
        spoken_languages = self._normalize_data(df, "spoken_languages", "tmdb_id")
        df.drop(columns=["spoken_languages"], inplace=True)
        
        return {
            'movies_metadata': df,
            'belongs_to_collection': belongs_to_collection,
            'genres': genres,
            'production_companies': production_companies,
            'production_countries': production_countries,
            'spoken_languages': spoken_languages
        }
    
    def _transform_ratings(self):
        """
        Transform and clean the ratings data. 
        
        Returns:
            tuple: DataFrames
        """
        df = self.ratings.copy()
        
        # Rename column using the same convention
        new_name = {
            "movieId": "movie_id",
            "userId": "user_id"
            }
        df.rename(columns=new_name, inplace=True)
        
        return df
        
    def _safe_parse_collection(self, x: pd.Series):
        """Safely parsing collection from JSON like Object into Dictionary."""
        if pd.isna(x):
            return np.nan
        
        if isinstance(x, dict):
            return x  # Already a dictionary, return as is
        try:
            return ast.literal_eval(x)
        except (ValueError, SyntaxError, TypeError):
            return np.nan
    
    def _normalize_data(
        self,
        df: pd.DataFrame, 
        subset_column: str, 
        id_column: str,
        id_prefix: Optional[str] = None
    ) -> pd.DataFrame:
        """Normalization of nested data using vectorized operations."""
        # Create working copy with only necessary columns
        working_df = df[[id_column, subset_column]].copy()
        
        # Vectorized parsing of nested data
        working_df[subset_column] = working_df[subset_column].apply(self._safe_parse_collection)
        
        # Filter valid entries and explode lists
        valid_mask = working_df[subset_column].apply(
            lambda x: isinstance(x, list) and len(x) > 0 and all(isinstance(i, dict) for i in x)
        )
        exploded_df = working_df[valid_mask].explode(subset_column)
        
        if exploded_df.empty:
            return pd.DataFrame()
        
        # Normalize nested dicts in vectorized manner
        normalized = pd.json_normalize(exploded_df[subset_column])
        
        # Add reference ID with type conversion for memory efficiency
        if id_prefix:
            id_col_name = f"{id_prefix}_{id_column}"
        else:
            id_col_name = id_column
        
        normalized[id_col_name] = exploded_df[id_column].astype(
            exploded_df[id_column].dtype
        ).values
        
        return normalized.reset_index(drop=True)

    def _extract_dict_values(
        self,
        df: pd.DataFrame, 
        column_name: str, 
        new_column_prefix: Optional[str] = None
    ) -> pd.DataFrame:
        """Optimized dictionary expansion using vectorized operations."""
        new_df = df.copy()
        
        # Remove column that has no value
        mask = new_df[[column_name]].isna().all(axis=1)
        new_df = new_df[~mask].reset_index()
        
        # Try to parse value
        parsed = new_df[column_name].apply(self._safe_parse_collection)
        normalized = pd.json_normalize(parsed)
        
        # Apply prefix if specified
        if new_column_prefix:
            normalized = normalized.add_prefix(f"{new_column_prefix}_")
        
        # Join results using pandas" efficient merge instead of row-wise operations
        return pd.concat([new_df, normalized], axis=1).drop(column_name, axis=1)