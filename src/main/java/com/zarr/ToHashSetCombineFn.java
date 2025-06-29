package com.zarr;

import org.apache.beam.sdk.transforms.Combine;

import java.util.HashSet;
import java.util.Set;

public class ToHashSetCombineFn<T> extends Combine.CombineFn<T, Set<T>, Set<T>> {
    @Override
    public Set<T> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public Set<T> addInput(Set<T> accumulator, T value) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public Set<T> mergeAccumulators(Iterable<Set<T>> accumulator){
        Set<T> merged = new HashSet<>();
        for(Set<T> set : accumulator){
            merged.addAll(set);
        }
        return merged;
    }

    @Override
    public Set<T> extractOutput(Set<T> accumulator) {
        return accumulator;
    }

}
