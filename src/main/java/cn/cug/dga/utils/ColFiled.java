package cn.cug.dga.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * author song
 * date 2024/2/28 22:58
 * Desc 封装列的字符串
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColFiled {

    private String comment;
    private String name;
    private String type;
}
