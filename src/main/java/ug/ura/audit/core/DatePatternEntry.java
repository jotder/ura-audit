package ug.ura.audit.core;

import java.util.regex.Pattern;

public record DatePatternEntry(
        String name,
        Pattern pattern,
        int yearGroup,
        int monthGroup,
        int dayGroup
) {}
