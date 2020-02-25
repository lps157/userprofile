package xyz.myself.userprofile.platform.repo;

import xyz.myself.userprofile.platform.bean.po.ModelPo;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ModelRepository  extends JpaRepository<ModelPo, Long> {

    ModelPo findByTagId(Long tagId);
}
