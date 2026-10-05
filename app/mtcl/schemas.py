"""Pydantic schemas cho module Mục tiêu chất lượng."""
from __future__ import annotations

from pydantic import BaseModel, Field


class MtclCompanyIn(BaseModel):
    nam: int = Field(..., ge=2000, le=2100)
    cscl: str = ""
    mtcl: str = ""
    note: str = ""


class MtclDepartIn(BaseModel):
    nam: int = Field(..., ge=2000, le=2100)
    id_depart: int
    content: str = ""
    note: str = ""


class MtclCompanyNode(BaseModel):
    id: int
    cscl: str
    mtcl: str
    note: str = ""


class MtclCsclGroup(BaseModel):
    cscl: str
    muc_tieu: list[MtclCompanyNode]


class MtclDepartNode(BaseModel):
    id: int
    id_depart: int
    ten: str
    mo_ta: str = ""
    content: str
    note: str = ""
    has_to_chuc: bool = False
    has_vai_tro: bool = False


class MtclOrgRole(BaseModel):
    """1 chức vụ của người trong ô — 1 ô có thể có nhiều role. vai_tro chỉ có khi
    tạo bằng luồng cũ (ghép sẵn theo dòng); luồng tick checkbox tạo vai trò riêng (xem MtclOrgVaiTroTag)."""
    detail_id: int | None = None
    chuc_vu_id: int | None = None
    chuc_vu: str = ""
    vai_tro_id: int | None = None
    vai_tro: str = ""


class MtclOrgVaiTroTag(BaseModel):
    """1 vai trò gắn cho cả ô (người), không gắn theo chức vụ cụ thể nào."""
    detail_id: int | None = None
    vai_tro_id: int
    vai_tro: str = ""


class MtclNodeLink(BaseModel):
    """1 link URL gắn vào khối sơ đồ."""
    id: int
    url: str = ""
    label: str = ""


class MtclOrgNode(BaseModel):
    """Ô trên canvas sơ đồ (danh sách phẳng, có tọa độ) = 1 người, có thể nhiều chức vụ/vai trò."""
    id: int
    detail_id: int | None = None
    name: str
    code: str = ""
    roles: list[MtclOrgRole] = []
    vai_tro_tags: list[MtclOrgVaiTroTag] = []
    links: list[MtclNodeLink] = []
    cap: int = 1
    reports_to: int | None = None
    cap_parent: int | None = None
    ho_tro: list[int] = []
    x: float = 0
    y: float = 0
    z_index: int = 0
    children: list["MtclOrgNode"] = []  # giữ tương thích; canvas dùng danh sách phẳng


class MtclOrgArrow(BaseModel):
    id: int
    kind: str
    shape_type: str = "arrow"  # arrow | line | rect | note
    color: str = "#1A1C1D"
    dash: str = ""  # rỗng = nét liền
    z_index: int = 0
    content: str = ""  # nội dung ghi chú (shape_type = note)
    from_node_id: int
    to_node_id: int
    x1: float = 0
    y1: float = 0
    x2: float = 0
    y2: float = 0
    points: list[list[float]] = []  # điểm gập khúc ở giữa, thứ tự từ (x1,y1) → (x2,y2); rect/line/note không dùng


class MtclOrgArrowCoordsIn(BaseModel):
    id: int
    x1: float
    y1: float
    x2: float
    y2: float
    points: list[list[float]] = []
    color: str | None = None
    dash: str | None = None


class MtclOrgArrowsUpdateIn(BaseModel):
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    arrows: list[MtclOrgArrowCoordsIn]


class MtclOrgPositionIn(BaseModel):
    node_id: int
    x: float
    y: float


class MtclOrgPositionsIn(BaseModel):
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    positions: list[MtclOrgPositionIn]


class MtclOrgLinkIn(BaseModel):
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    from_node_id: int
    to_node_id: int
    # "lien_ket" = loại nối gộp chung (chọn màu/kiểu tự do); cap/bao_cao/ho_tro giữ để tương thích dữ liệu cũ.
    kind: str = Field(..., pattern="^(cap|bao_cao|ho_tro|lien_ket|clear)$")
    x1: float | None = None
    y1: float | None = None
    x2: float | None = None
    y2: float | None = None
    points: list[list[float]] = []
    color: str = ""
    dash: str = ""


class MtclShapeCreateIn(BaseModel):
    """Vẽ 1 hình tự do (đoạn thẳng / ô vuông), không nhất thiết gắn vào khối nào."""
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    shape_type: str = Field(..., pattern="^(line|rect|arrow)$")
    x1: float
    y1: float
    x2: float
    y2: float
    color: str = "#1A1C1D"
    dash: str = ""


class MtclNoteItemIn(BaseModel):
    content: str
    color: str = "#1A1C1D"


class MtclNoteBatchCreateIn(BaseModel):
    """Thêm nhiều ghi chú cùng lúc, mỗi dòng 1 màu + 1 nội dung riêng.

    Ghi chú hiển thị trong bảng chú thích cố định (không phải hình vẽ trên canvas)
    nên không cần tọa độ.
    """
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    notes: list[MtclNoteItemIn]


class MtclNoteUpdateIn(BaseModel):
    """Sửa nội dung / màu 1 ghi chú đã có."""
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    note_id: int
    content: str
    color: str = "#1A1C1D"


class MtclNodeLinkCreateIn(BaseModel):
    """Thêm 1 link vào khối sơ đồ."""
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    so_do_id: int
    url: str
    label: str = ""


class MtclNodeLinkUpdateIn(BaseModel):
    """Sửa 1 link đã có."""
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    link_id: int
    url: str
    label: str = ""


class MtclNodeLinkDeleteIn(BaseModel):
    """Xóa 1 link."""
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    link_id: int


class MtclZOrderIn(BaseModel):
    """Đưa khối/mũi tên/hình vẽ lên trên hoặc xuống dưới trong nhóm cùng loại."""
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    target_type: str = Field(..., pattern="^(node|arrow)$")
    target_id: int
    action: str = Field(..., pattern="^(front|back|forward|backward)$")


class MtclOrgOption(BaseModel):
    id: int
    name: str
    code: str = ""


class MtclOrgCatalog(BaseModel):
    employees: list[MtclOrgOption] = []
    chuc_vu: list[MtclOrgOption] = []
    vai_tro: list[MtclOrgOption] = []


class MtclOrgNodeCreateIn(BaseModel):
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    employee_id: int | None = None
    employee_name: str = ""
    employee_code: str = ""
    chuc_vu_id: int | None = None
    chuc_vu_name: str = ""
    vai_tro_id: int | None = None
    vai_tro_name: str = ""
    cap: int = Field(1, ge=1, le=20)
    x: float = 80
    y: float = 80


class MtclOrgNodeBatchCreateIn(BaseModel):
    """Thêm 1 người vào sơ đồ, tick nhiều chức vụ + nhiều vai trò cùng lúc."""
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    employee_id: int | None = None
    employee_name: str = ""
    employee_code: str = ""
    chuc_vu_ids: list[int] = []
    chuc_vu_names: list[str] = []
    vai_tro_ids: list[int] = []
    vai_tro_names: list[str] = []
    cap: int = Field(1, ge=1, le=20)
    x: float = 80
    y: float = 80


class MtclOrgNodeUpdateIn(BaseModel):
    """Sửa toàn bộ nội dung 1 khối: đổi tên/mã NV + thay thế danh sách chức vụ/vai trò."""
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    so_do_id: int
    employee_name: str = ""
    employee_code: str = ""
    chuc_vu_ids: list[int] = []
    chuc_vu_names: list[str] = []
    vai_tro_ids: list[int] = []
    vai_tro_names: list[str] = []


class MtclOrgNodeDeleteIn(BaseModel):
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    node_id: int


class MtclOrgNodeDetailsDeleteIn(BaseModel):
    """Xóa các dòng chức vụ/vai trò cụ thể (undo thao tác Thêm ô), không xóa cả khối."""
    id_depart: int
    loai_so_do: int = Field(1, ge=1, le=2)
    so_do_id: int
    detail_ids: list[int]


class MtclDepartDetail(BaseModel):
    id_depart: int
    ten: str
    mo_ta: str = ""
    nam: int
    mtcl_id: int | None = None
    content: str = ""
    note: str = ""
    so_do_to_chuc: list[MtclOrgNode] = []
    so_do_vai_tro: list[MtclOrgNode] = []
    arrows_to_chuc: list[MtclOrgArrow] = []
    arrows_vai_tro: list[MtclOrgArrow] = []


class MtclDepartmentOption(BaseModel):
    id: int
    name: str
    description: str = ""


class MtclDiagramResponse(BaseModel):
    nam: int
    years: list[int]
    chien_luoc: list[MtclCsclGroup]
    don_vi: list[MtclDepartNode]
    depart_columns: list[list[MtclDepartNode]] = []
    departments: list[MtclDepartmentOption]
    company_count: int = 0
    depart_count: int = 0


MtclOrgNode.model_rebuild()
