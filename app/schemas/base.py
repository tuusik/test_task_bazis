from pydantic import BaseModel, ConfigDict


class SBase(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        extra="forbid",
        str_strip_whitespace=True,
    )
