package com.example.kafkastreamsconsumer.collaborators.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class GroupByKey {
    private Long id;
    private String desc;
}
