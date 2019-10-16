;(() => {
  const formats = ['json', 'txt']
  const jobId = key => key.split('/')[2]

  const createEntry = ({ key, ts }) => {
    const element = document.createElement('div')

    const title = document.createElement('span')
    const id = jobId(key)
    title.innerText = `${id} - ${new Date(ts).toLocaleString()}`
    element.appendChild(title)

    for (const format of formats) {
      const anchor = document.createElement('a')
      anchor.classList.add('download')
      anchor.href = `/jobs/${id}.${format}`
      anchor.innerText = `(${format})`
      element.appendChild(anchor)
    }

    return element
  }

  const emptyNode = selector => {
    const oldChild = document.querySelector(selector)
    const node = oldChild.cloneNode(false)
    node.classList.toggle('loader', true)
    window.requestAnimationFrame(() => {
      oldChild.parentNode.replaceChild(node, oldChild)
    })
    return node
  }

  const fillNode = (data, oldChild) => {
    const node = oldChild.cloneNode(false)
    node.classList.toggle('loader', false)
    if (data.length === 0) {
      const element = document.createElement('div')
      element.innerText = 'Nenhum'
      node.appendChild(element)
    } else {
      data.map(createEntry).map(e => node.append(e))
    }
    window.requestAnimationFrame(() => {
      oldChild.parentNode.replaceChild(node, oldChild)
    })
    return node
  }

  const showMessage = text => {
    const oldChild = document.querySelector('.message')
    const node = oldChild.cloneNode(false)
    const content = document.createElement('span')
    content.innerText = text
    node.appendChild(content)
    window.requestAnimationFrame(() => {
      oldChild.parentNode.replaceChild(node, oldChild)
    })
  }

  document.addEventListener('DOMContentLoaded', async e => {
    const reloadButton = document.querySelector('.reload')

    const reload = async () => {
      if (reloadButton.disabled) {
        return
      }
      reloadButton.disabled = true

      window.requestAnimationFrame(async () => {
        const runningSlot = emptyNode('.running .slot')
        const pendingSlot = emptyNode('.pending .slot')
        const finishedSlot = emptyNode('.finished .slot')

        const res = await fetch('/jobs/')
        const { running, pending, finished } = await res.json()

        window.requestAnimationFrame(() => {
          try {
            fillNode(running, runningSlot)
            fillNode(pending, pendingSlot)
            fillNode(finished, finishedSlot)
          } finally {
            reloadButton.disabled = false
          }
        })
      })
    }

    reloadButton.addEventListener('click', e => {
      e.preventDefault()
      reload()
    })

    const submitForm = async e => {
      e.preventDefault()
      const form = e.currentTarget
      const { action, method } = form
      const body = new FormData(form)
      const res = await fetch(action, {
        body,
        method,
      })

      if (res.ok) {
        const { key } = await res.json()
        showMessage(`Execução agendada! Código: ${jobId(key)}`)
        return reload()
      }

      if (res.status === 425) {
        return showMessage(
          'Já existe uma execução pendente para esta consulta!'
        )
      }

      showMessage(
        `Ocorreu um erro inesperado: ${res.status} ${res.statusText}.`
      )
    }

    document.querySelector('.form').addEventListener('submit', submitForm)

    reload()
  })
})()
