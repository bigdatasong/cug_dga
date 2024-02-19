package cn.cug.dga.meta.controller;

import cn.cug.dga.meta.service.TableMetaInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 * 元数据表 前端控制器
 * </p>
 *
 * @author song
 * @since 2024-02-07
 */
@RestController
@RequestMapping("/tableMetaInfo")
public class TableMetaInfoController {

    //手动更新元数据 http://dga.gmall.com/tableMetaInfo/init-tables/g/2024-01-29
    // 注入metainfoservice
    @Autowired
    private TableMetaInfoService tableMetaInfoService;
    @PostMapping("/init-tables/{schamaName}/{assessDate}")
    public String initMetaInfoTables(@PathVariable("schamaName") String schemaName,@PathVariable("assessDate") String assessDate) throws Exception {

        tableMetaInfoService.initMetaInfoTables(schemaName,assessDate); //该方法是不需要返回值的，因为我只是更新元数据后是要保存到数据库中

        return "success";
    }


}
