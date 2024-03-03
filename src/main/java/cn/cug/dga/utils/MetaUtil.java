package cn.cug.dga.utils;

import cn.cug.dga.ds.bean.TDsTaskInstance;
import cn.cug.dga.meta.bean.TableMetaInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * author song
 * date 2024/3/1 16:23
 * Desc
 */
@Component
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MetaUtil {

    private   Map<String, TableMetaInfo> tableMetaInfoMap = new HashMap<>();

    private  Map<String, TDsTaskInstance> tDsTaskInstanceMap = new HashMap<>();
}
