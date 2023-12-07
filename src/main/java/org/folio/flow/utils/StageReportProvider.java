package org.folio.flow.utils;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.flow.utils.CollectionUtils.isEmpty;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import lombok.Builder;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;
import org.folio.flow.model.StageResult;

@Builder
@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
public class StageReportProvider {

  private static final String DEFAULT_TEMPLATE = "-> ${stage} |> ${statusName}";
  private static final String ERROR_DEFAULT_TEMPLATE =
    "-> ${stage} |> ${statusName} with [${errorType}]: ${shortErrorMessage}";

  /**
   * Initial intend level.
   */
  @Builder.Default
  private final int initialIntendLevel = 1;

  /**
   * Intend size.
   */
  @Builder.Default
  private final int intendSize = 2;

  /**
   * Non-error line template.
   */
  @Builder.Default
  private final String template = DEFAULT_TEMPLATE;

  /**
   * Non-error line template.
   */
  @Builder.Default
  private final String errorTemplate = ERROR_DEFAULT_TEMPLATE;

  /**
   * Creates a report string for list with stage results.
   *
   * @param stageResults - stage results to analyze
   * @return {@link String} as stage execution report
   */
  public String create(List<StageResult> stageResults) {
    if (stageResults.isEmpty()) {
      return "".repeat(initialIntendLevel * intendSize) + "0 stages executed";
    }
    try {
      return create(stageResults, initialIntendLevel);
    } catch (Exception e) {
      return "Failed to create report: " + e.getClass().getSimpleName();
    }
  }

  private String create(List<StageResult> stageResults, int level) {
    if (isEmpty(stageResults)) {
      return "";
    }

    var stringJoiner = new StringJoiner("\n");
    for (StageResult stageResult : stageResults) {
      stringJoiner.add(getStageResultLine(stageResult, level));
      var failedSubStages = create(stageResult.getSubStageResults(), level + 1);
      if (isNotEmpty(failedSubStages)) {
        stringJoiner.add(failedSubStages);
      }
    }

    return stringJoiner.toString().stripTrailing();
  }

  public String getStageResultLine(StageResult stageResult, int level) {
    var parameters = getSubstitutionParameters(stageResult);
    var resultTemplate = stageResult.getError() != null ? errorTemplate : template;
    return " ".repeat(level * intendSize) + StrSubstitutor.replace(resultTemplate, parameters, "${", "}");
  }

  private static String getErrorType(StageResult sr) {
    var error = sr.getError();
    return error == null ? "null" : error.getClass().getSimpleName();
  }

  private static String getErrorMessage(StageResult sr, int maxLength) {
    var error = sr.getError();
    if (error == null) {
      return "null";
    }

    var errorMessage = error.getMessage();
    if (errorMessage == null) {
      return "null";
    }

    if (maxLength < 0) {
      return errorMessage;
    }

    return errorMessage.substring(0, Math.min(errorMessage.length(), maxLength));
  }

  private static Map<String, String> getSubstitutionParameters(StageResult stageResult) {
    var substitutionParameters = new HashMap<String, String>();

    substitutionParameters.put("flowId", stageResult.getFlowId());
    substitutionParameters.put("stage", stageResult.getStageName());
    substitutionParameters.put("status", stageResult.getStatus().getValue());
    substitutionParameters.put("statusName", stageResult.getStatus().name());
    substitutionParameters.put("errorType", getErrorType(stageResult));
    substitutionParameters.put("errorMessage", getErrorMessage(stageResult, -1));
    substitutionParameters.put("shortErrorMessage", getErrorMessage(stageResult, 25));

    return substitutionParameters;
  }
}
