package xyz.myself.userprofile.platform.service;


import xyz.myself.userprofile.platform.bean.dto.ModelDto;

public interface Engine {

    void startModel(ModelDto modelDto);
    void stopModel(ModelDto modelDto);
}
