from sentence_transformers import SentenceTransformer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SBERTEmbedder:
    def __init__(self, model_name="all-MiniLM-L6-v2"):
        logger.info(f"Loading SBERT model: {model_name}")
        self.model = SentenceTransformer(model_name)

    def embed_documents(self, documents):
        """
        documents: List[str]
        Returns: List of embeddings
        """
        if not documents:
            logger.warning("No documents provided for embedding.")
            return []

        logger.info(f"Embedding {len(documents)} documents...")
        return self.model.encode(documents, show_progress_bar=True)

    def embed_query(self, query):
        """
        query: str
        Returns: Single embedding vector
        """
        return self.model.encode([query])[0]


if __name__ == "__main__":
    embedder = SBERTEmbedder()
    texts = ["Nvidia stock surged today.", "Tesla released their Q1 earnings."]
    vectors = embedder.embed_documents(texts)
    print(vectors[0].shape)  # Should be (384,)
