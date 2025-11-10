from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, List
import re


# Phase 1: Rule-based categorization
MERCHANT_RULES: dict[str, Tuple[str, str]] = {
    # keyword -> (category, merchant_name)
    "starbucks": ("Food & Drink", "Starbucks"),
    "mcdonald": ("Food & Drink", "McDonald's"),
    "kfc": ("Food & Drink", "KFC"),
    "ubereats": ("Food & Drink", "Uber Eats"),
    "ubereat": ("Food & Drink", "Uber Eats"),
    "ubere": ("Transport", "Uber"),
    "uber": ("Transport", "Uber"),
    "bolt": ("Transport", "Bolt"),
    "airbnb": ("Travel", "Airbnb"),
    "delta": ("Travel", "Delta"),
    "amzn": ("Shopping", "Amazon"),
    "amazon": ("Shopping", "Amazon"),
    "walmart": ("Groceries", "Walmart"),
    "tesco": ("Groceries", "Tesco"),
    "netflix": ("Subscriptions", "Netflix"),
    "spotify": ("Subscriptions", "Spotify"),
    "apple.com/bill": ("Subscriptions", "Apple"),
    "apple.com": ("Subscriptions", "Apple"),
    "itunes": ("Subscriptions", "Apple"),
    "microsoft": ("Subscriptions", "Microsoft"),
    "google": ("Subscriptions", "Google"),
    "playstation": ("Entertainment", "PlayStation"),
    "steam": ("Entertainment", "Steam"),
    "shell": ("Transport", "Shell"),
    "chevron": ("Transport", "Chevron"),
    "bp": ("Transport", "BP"),
    "atm withdrawal": ("Cash & ATM", "ATM"),
    "withdrawal": ("Cash & ATM", "ATM"),
    "salary": ("Income", "Salary"),
    "payroll": ("Income", "Payroll"),
    "ttfl": ("Food & Drink", "Restaurant"),  # example bank code prefix
}


class RuleBasedCategorizer:
    def __init__(self, rules: Optional[dict[str, Tuple[str, str]]] = None) -> None:
        self.rules = rules or MERCHANT_RULES
        # Precompile regex patterns for each keyword
        self._patterns: List[Tuple[re.Pattern[str], Tuple[str, str]]] = []
        for keyword, value in self.rules.items():
            pattern = re.compile(re.escape(keyword), flags=re.IGNORECASE)
            self._patterns.append((pattern, value))

    def categorize(self, description: str) -> Tuple[str, Optional[str]]:
        if not description:
            return "Uncategorized", None
        for pattern, (category, merchant) in self._patterns:
            if pattern.search(description):
                return category, merchant
        return "Uncategorized", None


# Phase 2: ML-based categorization (scikit-learn)
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    import joblib  # type: ignore
except Exception:  # pragma: no cover - allow import even if not installed yet
    TfidfVectorizer = object  # type: ignore
    LogisticRegression = object  # type: ignore
    Pipeline = object  # type: ignore
    joblib = None  # type: ignore


@dataclass
class MLCategorizerConfig:
    model_path: str = "models/categorization_model.pkl"


class MLCategorizer:
    def __init__(self, config: Optional[MLCategorizerConfig] = None) -> None:
        self.config = config or MLCategorizerConfig()
        self.pipeline: Optional[Pipeline] = None

    def load_model(self) -> bool:
        if joblib is None:
            return False
        try:
            self.pipeline = joblib.load(self.config.model_path)  # type: ignore
            return True
        except Exception:
            self.pipeline = None
            return False

    def save_model(self) -> None:
        if joblib is None or self.pipeline is None:
            return
        joblib.dump(self.pipeline, self.config.model_path)  # type: ignore

    def train(self, descriptions: List[str], labels: List[str]) -> None:
        # Lazily construct pipeline
        self.pipeline = Pipeline(
            steps=[
                ("tfidf", TfidfVectorizer(ngram_range=(1, 2), min_df=2)),
                ("clf", LogisticRegression(max_iter=1000)),
            ]  # type: ignore
        )
        self.pipeline.fit(descriptions, labels)  # type: ignore

    def predict(self, description: str) -> Optional[str]:
        if self.pipeline is None:
            if not self.load_model():
                return None
        try:
            pred = self.pipeline.predict([description])[0]  # type: ignore
            return str(pred)
        except Exception:
            return None


class CategorizationService:
    def __init__(self, rules: Optional[dict[str, Tuple[str, str]]] = None, ml: Optional[MLCategorizer] = None) -> None:
        self.rule_based = RuleBasedCategorizer(rules)
        self.ml = ml or MLCategorizer()

    def categorize_description(self, description: str) -> Tuple[str, Optional[str], str]:
        """
        Returns (category, merchant_name, source)
        - source: 'rules' | 'ml' | 'default'
        """
        category, merchant = self.rule_based.categorize(description)
        if category != "Uncategorized":
            return category, merchant, "rules"
        ml_cat = self.ml.predict(description)
        if ml_cat:
            return ml_cat, merchant, "ml"
        return "Uncategorized", merchant, "default"


