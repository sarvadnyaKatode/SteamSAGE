import gradio as gr
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer


BASE_DIR = Path(__file__).parent

EMBEDDINGS_PATH = BASE_DIR / "02_embeddings_vectors.npy"
APPIDS_PATH = BASE_DIR / "02_embeddings_appids.csv"
METADATA_PATH = BASE_DIR / "01_game_embeddings_sample.csv"
SENTIMENT_MEDIA_PATH = BASE_DIR / "appid_recommendationid_numeric_score_header_image_background.csv"
SCREENSHOTS_PATH = BASE_DIR / "appid_screenshots.csv"



print("Loading embeddings...")
game_embeddings = np.load(EMBEDDINGS_PATH)

print("Loading appids...")
df_appids = pd.read_csv(APPIDS_PATH)
df_appids["appid"] = df_appids["appid"].astype(str)

print("Loading metadata...")
df_metadata = pd.read_csv(METADATA_PATH)
df_metadata["appid"] = df_metadata["appid"].astype(str)

print("Loading sentiment & media data...")
df_sentiment_media = pd.read_csv(
    SENTIMENT_MEDIA_PATH,
    engine="python",
    on_bad_lines="skip"
)
df_sentiment_media["appid"] = df_sentiment_media["appid"].astype(str)

# Average sentiment per app
df_sentiment = (
    df_sentiment_media
    .groupby("appid")["numeric_score"]
    .mean()
    .reset_index()
    .rename(columns={"numeric_score": "avg_sentiment_score"})
)
df_sentiment["avg_sentiment_score"] = df_sentiment["avg_sentiment_score"].round(2)

# Header & background images
df_images = (
    df_sentiment_media[["appid", "header_image", "background"]]
    .drop_duplicates("appid")
)

print("Loading screenshots...")
df_screenshots = pd.read_csv(
    SCREENSHOTS_PATH,
    engine="python",
    on_bad_lines="skip"
)
df_screenshots["appid"] = df_screenshots["appid"].astype(str)

df_screenshots = (
    df_screenshots
    .groupby("appid")["screenshot_url"]
    .apply(list)
    .reset_index()
    .rename(columns={"screenshot_url": "screenshots"})
)


df_search_index = (
    df_appids
    .merge(df_metadata, on="appid", how="left")
    .merge(df_sentiment, on="appid", how="left")
    .merge(df_images, on="appid", how="left")
    .merge(df_screenshots, on="appid", how="left")
)

df_search_index["name"] = df_search_index["name"].fillna("Unknown Game")


df_search_index = df_search_index.rename(
    columns={"price_usd": "price"}
)

print("Loading model...")
model = SentenceTransformer("BAAI/bge-m3")
print("Ready!")



def search_games(query, top_k):
    if not query.strip():
        return []

    query_vec = model.encode(query).reshape(1, -1)
    similarities = cosine_similarity(query_vec, game_embeddings)[0]

    top_indices = similarities.argsort()[-top_k:][::-1]
    results = df_search_index.iloc[top_indices].copy()

    results["similarity_score"] = similarities[top_indices].round(4)
    results["steam_url"] = (
        "https://store.steampowered.com/app/" + results["appid"] + "/"
    )

    # json type output
    return results[
        [
            "name",
            "steam_url",
            "similarity_score",
            "avg_sentiment_score",
            "primary_genre",
            "price",
            "currency",
            "header_image",
            "background",
            "screenshots",
            "short_description",
        ]
    ].fillna("Missing").to_dict(orient="records")

#create gradio ui 
with gr.Blocks(title="Steam Semantic Game Search") as demo:
    gr.Markdown("# 🎮 Steam Semantic Game Search")
    gr.Markdown("Search Steam games using natural language")

    query = gr.Textbox(
        label="Search query",
        placeholder="e.g. minecraft, open world survival games",
    )

    top_k = gr.Slider(
        minimum=1,
        maximum=20,
        value=5,
        step=1,
        label="Number of results",
    )

    search_btn = gr.Button("Search")

    output = gr.JSON(label="Search Results")

    search_btn.click(
        fn=search_games,
        inputs=[query, top_k],
        outputs=output,
    )

demo.launch()
