package org.folio.flow.api.models;

import org.folio.flow.api.Cancellable;
import org.folio.flow.api.Recoverable;
import org.folio.flow.api.Stage;

public interface RecoverableAndCancellableTestStage extends Stage, Cancellable, Recoverable {}
