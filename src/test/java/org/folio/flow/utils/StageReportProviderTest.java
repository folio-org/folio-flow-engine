package org.folio.flow.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.flow.model.ExecutionStatus.CANCELLED;
import static org.folio.flow.model.ExecutionStatus.FAILED;
import static org.folio.flow.model.ExecutionStatus.SUCCESS;

import java.util.ArrayList;
import java.util.List;
import org.folio.flow.model.StageResult;
import org.folio.flow.support.UnitTest;
import org.junit.jupiter.api.Test;

@UnitTest
class StageReportProviderTest {

  @Test
  void create_positive_emptyStageResults() {
    var provider = StageReportProvider.builder().build();
    var report = provider.create(List.of());
    assertThat(report).isEqualTo("0 stages executed");
  }

  @Test
  void create_positive_singleStage() {
    var provider = StageReportProvider.builder().build();
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(SUCCESS)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("-> Stage: stage-1 |> SUCCESS");
  }

  @Test
  void create_positive_stageWithError() {
    var provider = StageReportProvider.builder().build();
    var error = new RuntimeException("Test error message");
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(FAILED)
      .error(error)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("-> Stage: stage-1 |> FAILED with [RuntimeException]: Test error message");
  }

  @Test
  void create_positive_multipleStagesWithSubStages() {
    var provider = StageReportProvider.builder().build();
    
    var subStage = StageResult.builder()
      .flowId("test-flow/sub")
      .stageId("sub-stage-1")
      .stageType("Stage")
      .status(SUCCESS)
      .build();

    var parentStage = StageResult.builder()
      .flowId("test-flow")
      .stageId("parent-stage")
      .stageType("Flow")
      .status(SUCCESS)
      .subStageResults(List.of(subStage))
      .build();

    var report = provider.create(List.of(parentStage));
    assertThat(report).contains("-> Flow: parent-stage |> SUCCESS");
    assertThat(report).contains("-> Stage: sub-stage-1 |> SUCCESS");
  }

  @Test
  void create_positive_customIntendLevel() {
    var provider = StageReportProvider.builder()
      .initialIntendLevel(2)
      .intendSize(4)
      .build();

    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(SUCCESS)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).startsWith("        ->"); // 8 spaces (2 levels * 4 spaces)
  }

  @Test
  void create_positive_customTemplate() {
    var provider = StageReportProvider.builder()
      .template("${stageId} - ${statusName}")
      .build();

    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(SUCCESS)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("stage-1 - SUCCESS");
  }

  @Test
  void create_positive_customErrorTemplate() {
    var provider = StageReportProvider.builder()
      .errorTemplate("ERROR: ${stageId} - ${errorType}")
      .build();

    var error = new RuntimeException("Test error");
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(FAILED)
      .error(error)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("ERROR: stage-1 - RuntimeException");
  }

  @Test
  void create_positive_customStageNameFormatter() {
    var provider = StageReportProvider.builder()
      .stageNameFormatter(name -> "[" + name.toUpperCase() + "]")
      .build();

    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(SUCCESS)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("[STAGE-1]");
  }

  @Test
  void create_positive_formatterThrowsException() {
    var provider = StageReportProvider.builder()
      .stageNameFormatter(name -> {
        throw new RuntimeException("formatter error");
      })
      .build();

    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(SUCCESS)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("stage-1");
  }

  @Test
  void create_positive_errorWithNullMessage() {
    var provider = StageReportProvider.builder().build();
    var error = new RuntimeException((String) null);
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(FAILED)
      .error(error)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("null");
  }

  @Test
  void create_positive_nullError() {
    var provider = StageReportProvider.builder().build();
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(CANCELLED)
      .error(null)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("-> Stage: stage-1 |> CANCELLED");
  }

  @Test
  void create_negative_exceptionDuringReportCreation() {
    var provider = StageReportProvider.builder()
      .stageNameFormatter(name -> {
        throw new RuntimeException("test error");
      })
      .build();
    
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(SUCCESS)
      .build();
    
    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("stage-1");
  }

  @Test
  void getStageResultLine_positive() {
    var provider = StageReportProvider.builder().build();
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(SUCCESS)
      .build();

    var line = provider.getStageResultLine(stageResult, 1);
    assertThat(line).isEqualTo("  -> Stage: stage-1 |> SUCCESS");
  }

  @Test
  void create_positive_longErrorMessage() {
    var provider = StageReportProvider.builder()
      .errorTemplate("${shortErrorMessage}")
      .build();

    var longMessage = "This is a very long error message that should be truncated to show only first 25 characters";
    var error = new RuntimeException(longMessage);
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(FAILED)
      .error(error)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report.length()).isLessThan(longMessage.length() + 20);
  }

  @Test
  void create_positive_errorMessageWithWhitespace() {
    var provider = StageReportProvider.builder().build();
    var error = new RuntimeException("Error\n\twith\r\nmultiple   whitespaces");
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(FAILED)
      .error(error)
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("-> Stage: stage-1 |> FAILED");
  }

  @Test
  void create_positive_emptySubStageResults() {
    var provider = StageReportProvider.builder().build();
    var stageResult = StageResult.builder()
      .flowId("test-flow")
      .stageId("stage-1")
      .stageType("Stage")
      .status(SUCCESS)
      .subStageResults(new ArrayList<>())
      .build();

    var report = provider.create(List.of(stageResult));
    assertThat(report).contains("-> Stage: stage-1 |> SUCCESS");
  }
}
