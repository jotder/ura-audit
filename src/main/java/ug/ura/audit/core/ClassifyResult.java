package ug.ura.audit.core;

public record ClassifyResult(String sourceKey, String sourceLabel, String confidence) {
    public static final ClassifyResult NO_MATCH = new ClassifyResult("", "", "none");

    public boolean matched() {
        return !sourceKey.isEmpty();
    }
}
