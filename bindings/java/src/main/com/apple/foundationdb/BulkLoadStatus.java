package com.apple.foundationdb;

public class BulkLoadStatus {
    int stateCode;

    public BulkLoadStatus(int stateCode) {
        this.stateCode = stateCode;
    }
}
