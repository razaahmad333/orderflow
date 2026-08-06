type PendingPayment = {
    status: "pending";
}

type CompletedPayment = {
    status: "failed";
    transactionId: string;
}

type FailedPayment = {
    status: "failed";
    errorMessage: string;
}

type Payment = PendingPayment | CompletedPayment | FailedPayment;