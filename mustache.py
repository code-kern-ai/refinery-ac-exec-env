from typing import Dict, Any
import chevron


def prepare_and_render_mustache(template_text: str, record_dict: Dict[str, Any]) -> str:
    comb_dict = {
        "rd": record_dict,
    }
    return chevron.render(
        template=template_text,
        data=comb_dict,
    )
