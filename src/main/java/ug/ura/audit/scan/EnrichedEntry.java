package ug.ura.audit.scan;

import ug.ura.audit.core.ClassifyResult;
import ug.ura.audit.core.DateResult;
import java.time.LocalDate;

public record EnrichedEntry(
        FileEntry file,
        ClassifyResult classification,
        DateResult dateResult
) {
    public String sourceKey() { return classification.sourceKey(); }
    public String sourceLabel() { return classification.sourceLabel(); }
    public String confidence() { return classification.confidence(); }
    public LocalDate date() { return dateResult.date(); }
    public String dateSource() { return dateResult.dateSource(); }
}
