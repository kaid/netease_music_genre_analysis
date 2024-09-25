import io
import base64
import gradio as gr
from PIL import Image, ImageFile

import utils.api as api

def display():
    with gr.Blocks() as demo:
        ui()

def ui():
    login_status = gr.State('init')
    check_count = gr.State(0)
    qr_key: gr.State[str] = gr.State(None)
    check_code: gr.State[int] = gr.State(None)
    check_cookie: gr.State[str] = gr.State(None)
    qr_image: gr.State[ImageFile.ImageFile] = gr.State(None)

    async def display_qr_image(login_status_s: str, qr_key_s: str, qr_image_s: ImageFile.ImageFile, check_code_s: int, check_count_s: int, check_cookie_s: str)-> (str, str, Image, str, str, int, dict):
        match login_status_s:
            case 'init':
                image_data, key = await api.get_qr_image_url()
                image = Image.open(io.BytesIO(base64.b64decode(image_data.split(',')[1])))
                return 'pending', key, image, None, None, check_count_s + 1, gr.update(interactive=False)
            case 'pending' if qr_key_s:
                (next_status, code, cookie) = await api.is_qr_login_success(qr_key_s)
                return next_status, qr_key_s, qr_image_s, code, cookie, check_count_s + 1, gr.update(interactive=False)
            case 'success' | 'failed':
                if login_status_s == 'success':
                    gr.Info('登录成功')
                else:
                    gr.Warning('登录失败')

                return 'success', qr_key_s, None, check_code_s, check_cookie_s, check_count_s, gr.update(interactive=True)

    @gr.render(inputs=[login_status, qr_key, qr_image, check_code, check_cookie, check_count])
    def components(login_status_s: str, qr_key_s: str, qr_image_s: ImageFile.ImageFile, check_code_s: int, check_cookie_s: str, check_count_s: int):
        gr.Image(interactive=False, value=qr_image_s)
        gr.Text(value=login_status_s, label='login_status')
        gr.Text(value=qr_key_s, label='qr_code')
        gr.Number(value=check_code_s, label='check_code')
        gr.Text(value=check_cookie_s, label='check_cookie')
        gr.Number(value=check_count_s, label='check_count')
        restart = gr.Button('Restart', visible=login_status_s != 'init')
        restart.click(
            lambda: ('init', None, None, None, None, 0),
            cancels=[poll],
            outputs=[login_status, qr_key, qr_image, check_code, check_cookie, check_count],
        )

    login_button = gr.Button('QR Login')
    poll = login_button.click(
        display_qr_image,
        every=4.0,
        trigger_mode='always_last',
        inputs=[login_status, qr_key, qr_image, check_code, check_count, check_cookie],
        outputs=[login_status, qr_key, qr_image, check_code, check_cookie, check_count, login_button],
    )