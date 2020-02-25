package xyz.myself.userprofile.platform.service;

import xyz.myself.userprofile.platform.bean.dto.ModelDto;
import xyz.myself.userprofile.platform.bean.dto.TagDto;
import xyz.myself.userprofile.platform.bean.dto.TagModelDto;

import java.util.List;

public interface TagService {
    public void saveTags(List<TagDto> tags);

    List<TagDto> findByPid(Long pid);

    List<TagDto> findByLevel(Integer level);

    void addTagModel(TagDto tag, ModelDto model);

    List<TagModelDto> findModelByPid(Long pid);

    void addDataTag(TagDto tagDto);

    void updateModelState(Long id, Integer state);
}
