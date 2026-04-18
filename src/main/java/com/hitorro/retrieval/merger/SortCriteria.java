package com.hitorro.retrieval.merger;

/**
 * Describes how search results should be sorted when merging from multiple providers.
 */
public record SortCriteria(String field, Direction direction, SortType type) {

    public enum Direction { ASC, DESC }
    public enum SortType { SCORE, FIELD, CUSTOM }

    public static SortCriteria byScore() {
        return new SortCriteria("_score", Direction.DESC, SortType.SCORE);
    }

    public static SortCriteria byField(String field, Direction direction) {
        return new SortCriteria(field, direction, SortType.FIELD);
    }
}
